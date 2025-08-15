# app/routes.py
import json
import time
import traceback
import asyncio
import httpx
from quart import request, jsonify, render_template, current_app
from p115client.tool.util import share_extract_payload
from cachetools import TTLCache
from sqlalchemy import func, select, update, delete

from app import app, p115_client, APP_CONFIG, http_client, task_lock
from app.config import save_config
from app.tasks import check_share_status_task, master_cleanup_task, schedule_task, _sync_to_cms
from app.notifications import send_feishu_notification_from_emby
from app.database import get_session, Shares

# --- 缓存实例 ---
browse_cache = TTLCache(maxsize=512, ttl=120)

@app.route('/')
async def index():
    return await render_template('index.html')

@app.route('/api/config', methods=['GET', 'POST'])
async def api_config():
    if request.method == 'GET':
        config_to_return = {k: v for k, v in APP_CONFIG.items() if k.islower()}
        return jsonify(config_to_return)
    elif request.method == 'POST':
        data = await request.get_json()
        APP_CONFIG.update(data)
        save_config(APP_CONFIG)
        schedule_task() 
        return jsonify({"success": True, "message": "设置已保存并计划任务已更新。"})

@app.route('/api/logs')
async def api_logs():
    try:
        with open('app.log', 'r', encoding='utf-8') as f:
            lines = f.readlines()
            last_lines = lines[-300:]
            return jsonify({"logs": "".join(last_lines)})
    except FileNotFoundError:
        return jsonify({"logs": "日志文件尚未创建。"})

@app.route('/api/logs/clear', methods=['POST'])
async def api_clear_app_log():
    try:
        with open('app.log', 'w', encoding='utf-8') as f:
            f.truncate(0)
        return jsonify({"success": True, "message": "运行时日志已清空。"})
    except FileNotFoundError:
        return jsonify({"success": True, "message": "日志文件不存在，无需操作。"})
    except Exception as e:
        return jsonify({"error": f"清空日志失败: {e}"}), 500

@app.route('/api/share-log/clear', methods=['POST'])
async def api_clear_share_log():
    async with task_lock:
        try:
            async with get_session() as session:
                delete_stmt = delete(Shares)
                await session.execute(delete_stmt)
            print("[WARN] 数据库 shares 表已被清空。")
            return jsonify({"success": True, "message": "所有分享记录已清空。"})
        except Exception as e:
            return jsonify({"error": f"清空分享记录失败: {e}"}), 500

@app.route('/api/cms-login', methods=['POST'])
async def api_cms_login():
    data = await request.get_json() # <--- 必须 await
    domain, username, password = data.get('cms_domain'), data.get('cms_username'), data.get('cms_password')
    
    if not all([domain, username, password]):
        return jsonify({"error": "CMS信息不完整"}), 400
    
    try:
        r = await http_client.post(f"{domain.rstrip('/')}/api/auth/login", json={"username": username, "password": password}, timeout=10) # <--- 必须 await
        r.raise_for_status()
        cms_data = r.json()
        
        if cms_data.get("code") == 200 and cms_data.get("data", {}).get("token"):
            APP_CONFIG.update({"cms_token": cms_data["data"]["token"]})
            save_config(APP_CONFIG)
            return jsonify({"success": True, "message": "CMS 登录成功！", "cms_token": APP_CONFIG['cms_token']})
        else:
            return jsonify({"error": f"CMS 登录失败: {cms_data.get('msg', '未知错误')}"}), 400
            
    except httpx.RequestError as e:
        return jsonify({"error": f"连接 CMS 失败: {e}"}), 500
    except Exception as e:
        # 捕获其他可能的异常，例如 r.json() 失败
        return jsonify({"error": f"处理CMS响应时出错: {e}"}), 500

@app.route('/api/cms-sync-manual', methods=['POST'])
async def api_cms_sync_manual():
    data = await request.get_json()
    share_url = data.get('share_url')
    async with task_lock:
        try:
            async with get_session() as session:
                query = select(Shares).filter_by(share_url=share_url)
                result = await session.execute(query)
                entry = result.scalar_one_or_none()
                
                if not entry:
                    return jsonify({"error": "未在数据库中找到该分享"}), 404
                
                if str(entry.status) != '1':
                    return jsonify({"error": "该分享状态不是'正常'"}), 400
                
                entry_dict = {c.name: getattr(entry, c.name) for c in entry.__table__.columns}
                success, message = await _sync_to_cms(entry_dict)

                if success:
                    entry.status = 4
                    return jsonify({"success": True, "message": "手动入库成功！"})
                else:
                    return jsonify({"error": f"入库失败: {message}"}), 500
        except Exception as e:
            return jsonify({"error": f"数据库操作失败: {e}"}), 500

@app.route('/api/browse')
async def api_browse():
    cid = request.args.get('cid', APP_CONFIG.get('root_cid'))
    
    if cid in browse_cache:
        print(f"[CACHE] 命中缓存: CID {cid}")
        return jsonify(browse_cache[cid])

    if not p115_client: 
        return jsonify({"error": "服务器端115客户端未初始化"}), 500
    try:
        list_response = await asyncio.to_thread(p115_client.fs_files_app, {"cid": cid, "limit": 7000, "o": "file_name", "fc_mix": 0, "asc": 1})
        if not list_response.get("state"): 
            return jsonify({"error": f"从115获取文件列表失败: {list_response.get('error', '未知API错误')}"}), 500
        
        files_to_return = []
        for item in list_response.get("data", []):
            files_to_return.append({
                "id": str(item.get('cid') or item.get('fid') or item.get('file_id')),
                "name": item.get("n") or item.get("fn") or item.get("file_name"),
                "time": time.strftime("%Y-%m-%d %H:%M", time.localtime(int(item.get("t") or item.get("upt") or 0))),
                "is_dir": "s" not in item and "fs" not in item
            })
        
        response_data = {"files": files_to_return}
        browse_cache[cid] = response_data
        return jsonify(response_data)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"服务器内部发生意外错误。"}), 500

@app.route('/api/my-shares')
async def api_my_shares():
    offset = request.args.get('offset', 0, type=int)
    limit = request.args.get('limit', 20, type=int)
    status_filter = request.args.get('status_filter', '')
    
    async with get_session() as session:
        total_query = select(func.count()).select_from(Shares)
        if status_filter:
            total_query = total_query.filter_by(status=int(status_filter))
        total_result = await session.execute(total_query)
        total = total_result.scalar_one()

        query = select(Shares)
        if status_filter:
            query = query.filter_by(status=int(status_filter))
        query = query.order_by(Shares.timestamp.desc()).limit(limit).offset(offset)
        result = await session.execute(query)
        paginated_entries = result.scalars().all()

    shares_to_return = [{"share_url": i.share_url, "receive_code": i.receive_code, "file_name": i.share_title, "create_time": i.timestamp, "status": i.status} for i in paginated_entries]
    return jsonify({"shares": shares_to_return, "total": total})

@app.route('/api/create-share', methods=['POST'])
async def api_create_share():
    data = await request.get_json()
    file_ids, current_path, names = data.get('file_ids'), data.get('current_path'), data.get('selected_names')
    if not file_ids:
        return jsonify({"error": "未选择任何文件"}), 400
    
    r = await asyncio.to_thread(p115_client.share_send_app, {"file_ids": ",".join(file_ids), "ignore_warn": 1})
    if not r.get("state"):
        return jsonify({"error": f"创建分享失败: {r.get('error')}"}), 500
        
    d = r.get("data", {})
    await asyncio.to_thread(p115_client.share_update_app, {"share_code": d.get("share_code"), "share_duration": -1})
    
    new_share = Shares(
        timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
        path_hierarchy=json.dumps(current_path),
        shared_cids=json.dumps(file_ids),
        share_title=" / ".join(names),
        share_url=d.get("share_url"),
        share_code=d.get("share_code"),
        receive_code=d.get("receive_code"),
        status=0
    )
    async with get_session() as session:
        session.add(new_share)

    return jsonify({"new_url": new_share.share_url, "password": new_share.receive_code})

@app.route('/api/delete-share', methods=['POST'])
async def api_delete_share():
    data = await request.get_json()
    url = data.get('share_url')
    async with task_lock:
        async with get_session() as session:
            query = delete(Shares).where(Shares.share_url == url)
            await session.execute(query)
    return jsonify({"success": True})

@app.route('/api/manual-audit', methods=['POST'])
async def api_manual_audit():
    try:
        await check_share_status_task()
        return jsonify({"success": True, "message": "审核任务执行完成。"})
    except Exception as e:
        return jsonify({"error": f"内部错误: {e}"}), 500

@app.route('/api/manual-clean', methods=['POST'])
async def api_manual_clean():
    try:
        await master_cleanup_task()
        return jsonify({"success": True, "message": "清理任务执行完成。"})
    except Exception as e:
        return jsonify({"error": f"内部错误: {e}"}), 500

@app.route('/api/transfer-share', methods=['POST'])
async def api_transfer_share():
    if not p115_client:
        return jsonify({"error": "115客户端未初始化"}), 500
    
    data = await request.get_json()
    original_link = data.get('shareLink')
    category_name = data.get('category')
    if not original_link or not category_name:
        return jsonify({"error": "分享链接和类型不能为空"}), 400

    target_cid = APP_CONFIG.get('target_cid', '0')
    transferred_file_ids = []
    try:
        parsed_info = await asyncio.to_thread(share_extract_payload, original_link)
        if not isinstance(parsed_info, dict) or "share_code" not in parsed_info:
            raise ValueError("无法从URL中解析出有效的分享信息")
        
        receive_payload = {"share_code": parsed_info['share_code'], "receive_code": parsed_info.get('receive_code', ""),"cid": target_cid}
        receive_data = await asyncio.to_thread(p115_client.share_receive_app, receive_payload, app='android')
        if not receive_data.get('state'):
            raise ValueError(f"转存失败: {receive_data.get('error', '未知错误')}")
        
        await asyncio.sleep(5) 
        list_data = (await asyncio.to_thread(p115_client.fs_files_app, {"cid": target_cid, "limit": 10}, app='android')).get('data', [])
        if not list_data:
            raise ValueError("在临时目录中未找到转存的文件")
        
        transferred_file_ids = [str(item.get('fid') or item.get('cid')) for item in list_data]
        file_names = [item.get('fn') for item in list_data]
        
        share_send_data = await asyncio.to_thread(p115_client.share_send_app, {"file_ids": ",".join(transferred_file_ids), "ignore_warn": 1})
        if not share_send_data.get('state'):
            raise ValueError(f"创建新分享失败: {share_send_data.get('error')}")
        
        share_data = share_send_data.get('data', {})
        new_share_code = share_data.get('share_code')
        if not new_share_code:
            raise ValueError("创建新分享后未能获取分享码")
        
        await asyncio.to_thread(p115_client.share_update_app, {"share_code": new_share_code, "share_duration": -1})
        
        new_share = Shares(
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
            path_hierarchy=json.dumps([{"name": category_name}]),
            shared_cids=json.dumps(transferred_file_ids),
            share_title=" / ".join(file_names),
            share_url=share_data.get("share_url"),
            share_code=new_share_code,
            receive_code=share_data.get("receive_code"),
            status=0
        )
        async with get_session() as session:
            session.add(new_share)
        
        result_data = {c.name: getattr(new_share, c.name) for c in new_share.__table__.columns if c.name not in ['_sa_instance_state']}
        return jsonify({"success": True, "data": result_data})
        
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if transferred_file_ids:
            try:
                await asyncio.sleep(5) 
                await asyncio.to_thread(p115_client.fs_delete, ",".join(transferred_file_ids))
                print(f"[INFO] 临时目录 {transferred_file_ids} 清理完毕。")
            except Exception as e:
                print(f"[ERROR] 清理临时目录 {transferred_file_ids} 失败: {e}")

@app.route('/emby-webhook', methods=['POST'])
async def handle_emby_webhook():
    token = request.args.get('token')
    expected_token = APP_CONFIG.get('emby_webhook_token')

    if not expected_token or token != expected_token:
        return jsonify({"error": "无效或缺失的 Token"}), 403

    try:
        raw_payload = await request.get_json()
    except Exception:
        return jsonify({"error": "请求体不是有效的 JSON 格式"}), 400

    event_type = raw_payload.get("Event")
    if event_type in ["library.new", "library.deleted"]:
        asyncio.create_task(send_feishu_notification_from_emby(raw_payload))
        return jsonify({"status": "success", "message": "通知已收到"})
    else:
        return jsonify({"status": "skipped", "message": f"Event '{event_type}' is not handled."})