# app/routes.py
import json
import time
import traceback
import asyncio
import httpx
import logging
from quart import request, jsonify, render_template, current_app
from p115client.tool.util import share_extract_payload
from cachetools import TTLCache
from sqlalchemy import func, select, update, delete, or_

from app import app, p115_client, APP_CONFIG, http_client, task_lock
from app.config import save_config
from app.tasks import check_share_status_task, master_cleanup_task, schedule_task, _sync_to_cms
from app.notifications import send_feishu_notification_from_emby
from app.database import get_session, Shares

log = logging.getLogger(__name__)

# --- 缓存实例 ---
browse_cache = TTLCache(maxsize=512, ttl=120)


# --- 辅助函数：单个分享状态检查逻辑 ---
async def _check_single_share_status(share: Shares):
    if not p115_client:
        return False, "115客户端未初始化"
    try:
        if str(share.status) != '1':
            log.debug(f"正在检查分享: {share.share_title}")
            info_data = await asyncio.to_thread(p115_client.share_info_app, {"share_code": share.share_code})
            if info_data.get("state"):
                new_status = info_data.get("data", {}).get("share_state")
                if new_status != share.status:
                    share.status = new_status
                    log.info(f"分享状态变更: '{share.share_title}' -> [{new_status}]")
            elif "已取消" in info_data.get('error', ''):
                share.status = 3 # 已失效
                log.info(f"分享状态变更: '{share.share_title}' -> [已失效]")
        return True, "检查完成"
    except Exception as e:
        log.error(f"检查分享 {share.share_url} 时失败", exc_info=True)
        return False, str(e)

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
        log.info("配置已保存，定时任务已刷新。")
        return jsonify({"success": True, "message": "设置已保存并计划任务已更新。"})

@app.route('/api/logs')
async def api_logs():
    try:
        with open(APP_CONFIG.get("log_file", "data/app.log"), 'r', encoding='utf-8') as f:
            lines = f.readlines()
            last_lines = lines[-300:]
            return jsonify({"logs": "".join(last_lines)})
    except FileNotFoundError:
        return jsonify({"logs": "日志文件尚未创建。"})

@app.route('/api/logs/clear', methods=['POST'])
async def api_clear_app_log():
    try:
        log_file_path = APP_CONFIG.get("log_file", "data/app.log")
        with open(log_file_path, 'w', encoding='utf-8') as f:
            f.truncate(0)
        log.warning("管理员手动清空了运行时日志。")
        return jsonify({"success": True, "message": "运行时日志已清空。"})
    except FileNotFoundError:
        return jsonify({"success": True, "message": "日志文件不存在，无需操作。"})
    except Exception as e:
        log.error("清空日志文件失败", exc_info=True)
        return jsonify({"error": f"清空日志失败: {e}"}), 500

@app.route('/api/share-log/clear', methods=['POST'])
async def api_clear_share_log():
    async with task_lock:
        try:
            async with get_session() as session:
                delete_stmt = delete(Shares)
                await session.execute(delete_stmt)
            log.warning("【高危】管理员手动清空了所有分享记录。")
            return jsonify({"success": True, "message": "所有分享记录已清空。"})
        except Exception as e:
            log.error("清空分享记录失败", exc_info=True)
            return jsonify({"error": f"清空分享记录失败: {e}"}), 500

@app.route('/api/cms-login', methods=['POST'])
async def api_cms_login():
    data = await request.get_json()
    domain, username, password = data.get('cms_domain'), data.get('cms_username'), data.get('cms_password')
    
    if not all([domain, username, password]):
        return jsonify({"error": "CMS信息不完整"}), 400
    
    try:
        r = await http_client.post(f"{domain.rstrip('/')}/api/auth/login", json={"username": username, "password": password}, timeout=10)
        r.raise_for_status()
        cms_data = r.json()
        
        if cms_data.get("code") == 200 and cms_data.get("data", {}).get("token"):
            APP_CONFIG.update({"cms_token": cms_data["data"]["token"]})
            save_config(APP_CONFIG)
            return jsonify({"success": True, "message": "CMS 登录成功！", "cms_token": APP_CONFIG['cms_token']})
        else:
            return jsonify({"error": f"CMS 登录失败: {cms_data.get('msg', '未知错误')}"}), 400
            
    except httpx.RequestError as e:
        log.error(f"连接 CMS 失败: {e}")
        return jsonify({"error": f"连接 CMS 失败: {e}"}), 500
    except Exception as e:
        log.error("处理CMS响应时出错", exc_info=True)
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
            log.error(f"手动同步数据库操作失败", exc_info=True)
            return jsonify({"error": f"数据库操作失败: {e}"}), 500

@app.route('/api/browse')
async def api_browse():
    cid = request.args.get('cid', APP_CONFIG.get('root_cid'))
    # 接收分页参数，并设置默认值
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 50, type=int)
    offset = (page - 1) * page_size

    # 修改缓存键以包含页码
    cache_key = f"browse_{cid}_p{page}"
    
    if cache_key in browse_cache:
        log.debug(f"缓存命中: CID {cid}, Page {page}")
        return jsonify(browse_cache[cache_key])

    if not p115_client: 
        return jsonify({"error": "服务器端115客户端未初始化"}), 500
    try:
        # 构造带有分页参数的 payload
        payload = {
            "cid": cid, 
            "limit": page_size,
            "offset": offset,
            "o": "file_name", 
            "fc_mix": 0, 
            "asc": 1
        }
        list_response = await asyncio.to_thread(p115_client.fs_files_app, payload)
        
        if not list_response.get("state"): 
            return jsonify({"error": f"从115获取文件列表失败: {list_response.get('error', '未知API错误')}"}), 500
        
        items_from_115 = list_response.get("data", [])
        # 从API响应中获取文件总数
        total_count = list_response.get("count", 0)
        
        item_cids = [str(item.get('cid') or item.get('fid') or item.get('file_id')) for item in items_from_115]
        
        shared_cids_set = set()
        if item_cids:
            async with get_session() as session:
                conditions = [Shares.shared_cids.like(f'%"{cid_str}"%') for cid_str in item_cids]
                query = select(Shares.shared_cids).filter(or_(*conditions))
                result = await session.execute(query)
                for shared_cids_json in result.scalars().all():
                    try:
                        cids_in_share = json.loads(shared_cids_json)
                        shared_cids_set.update(cids_in_share)
                    except (json.JSONDecodeError, TypeError):
                        continue

        files_to_return = []
        for item in items_from_115:
            current_cid = str(item.get('cid') or item.get('fid') or item.get('file_id'))
            files_to_return.append({
                "id": current_cid,
                "name": item.get("n") or item.get("fn") or item.get("file_name"),
                "time": time.strftime("%Y-%m-%d %H:%M", time.localtime(int(item.get("t") or item.get("upt") or 0))),
                "is_dir": "s" not in item and "fs" not in item,
                "is_shared": current_cid in shared_cids_set
            })
        response_data = {"files": files_to_return, "total": total_count}
        browse_cache[cache_key] = response_data
        return jsonify(response_data)
    except Exception as e:
        log.error(f"浏览 CID {cid} 时发生意外错误", exc_info=True)
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
        
    shares_to_return = [{"share_url": i.share_url, "receive_code": i.receive_code, "file_name": i.share_title, "create_time": i.timestamp, "status": i.status, "path_hierarchy": i.path_hierarchy} for i in paginated_entries]
    return jsonify({"shares": shares_to_return, "total": total})

@app.route('/api/create-share', methods=['POST'])
async def api_create_share():
    data = await request.get_json()
    file_ids = data.get('file_ids', [])
    current_path = data.get('current_path')
    selected_names = data.get('selected_names', [])

    if not file_ids:
        return jsonify({"error": "未选择任何文件"}), 400
    
    if len(file_ids) != len(selected_names):
        return jsonify({"error": "文件ID和文件名的数量不匹配，无法创建分享。"}), 400

    selected_items = list(zip(file_ids, selected_names))
    CHUNK_SIZE = 5
    created_shares_result = []
    
    # --- 主循环开始 ---
    for i in range(0, len(selected_items), CHUNK_SIZE):
        # 步骤 1: 取出一批文件
        chunk = selected_items[i:i + CHUNK_SIZE]
        chunk_file_ids = [item[0] for item in chunk]
        chunk_names = [item[1] for item in chunk]
        
        log.info(f"开始创建第 {i//CHUNK_SIZE + 1} 批分享，包含 {len(chunk_names)} 个文件...")

        try:
            # 步骤 2: 调用 115 API 创建分享
            r = await asyncio.to_thread(
                p115_client.share_send_app, 
                {"file_ids": ",".join(chunk_file_ids), "ignore_warn": 1}
            )
            
            # 步骤 3: 检查 API 调用结果
            if not r.get("state"):
                error_msg = f"创建分享失败: {r.get('error')}"
                log.error(error_msg)
                # 如果失败，立即返回，created_shares_result 中包含了已经成功入库的数据
                return jsonify({"error": error_msg, "created_shares": created_shares_result}), 500
            
            # 调用成功，继续处理
            d = r.get("data", {})
            await asyncio.to_thread(
                p115_client.share_update_app, 
                {"share_code": d.get("share_code"), "share_duration": -1}
            )
            
            # 步骤 4: 构造 ORM 对象
            new_share = Shares(
                timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
                path_hierarchy=json.dumps(current_path),
                shared_cids=json.dumps(chunk_file_ids),
                share_title=" / ".join(chunk_names),
                share_url=d.get("share_url"),
                share_code=d.get("share_code"),
                receive_code=d.get("receive_code"),
                status=0
            )

            # 步骤 5: 写入数据库并提交
            # get_session 上下文管理器会在 with 块成功结束后自动提交事务
            async with get_session() as session:
                session.add(new_share)
            
            log.info(f"成功创建分享并已存入数据库: {new_share.share_title}")

            # 步骤 6: 收集成功结果
            created_shares_result.append({
                "new_url": new_share.share_url, 
                "password": new_share.receive_code,
                "title": new_share.share_title
            })

            # 步骤 7: 执行延时 (如果不是最后一批)
            if i + CHUNK_SIZE < len(selected_items):
                log.debug("批次间休眠 3 秒...")
                await asyncio.sleep(3)

        except Exception as e:
            error_msg = f"处理分享批次时发生意外错误: {e}"
            log.error(error_msg, exc_info=True)
            return jsonify({"error": error_msg, "created_shares": created_shares_result}), 500
    # --- 主循环结束 ---

    # 循环结束后的操作 1: 清理缓存
    if created_shares_result:
        if current_path:
            parent_cid = current_path[-1]['cid']
            cache_key = f"browse_{parent_cid}"
            browse_cache.pop(cache_key, None)
            log.info(f"因创建分享，清除了目录缓存: {cache_key}")
        else:
            root_cid = APP_CONFIG.get('root_cid', '0')
            cache_key = f"browse_{root_cid}"
            browse_cache.pop(cache_key, None)
            log.info(f"因创建分享，清除了根目录缓存: {cache_key}")

    log.info(f"全部分享创建完成，共生成 {len(created_shares_result)} 个新的分享链接。")
    
    # 循环结束后的操作 2: 返回最终结果
    return jsonify({
        "success": True, 
        "message": f"操作完成，成功创建 {len(created_shares_result)} 个分享链接。",
        "created_shares": created_shares_result
    })

# --- 变更: 修改 /api/delete-share 接口以清理缓存 ---
@app.route('/api/delete-share', methods=['POST'])
async def api_delete_share():
    data = await request.get_json()
    url = data.get('share_url')
    async with task_lock:
        async with get_session() as session:
            # 1. 首先，检索条目以获取其路径信息
            entry_to_delete = await session.get(Shares, url)
            if entry_to_delete:
                # 2. 清理其父目录的缓存
                try:
                    path_hierarchy = json.loads(entry_to_delete.path_hierarchy)
                    if path_hierarchy:
                        # 分享自某个子目录
                        parent_cid = path_hierarchy[-1]['cid']
                        cache_key = f"browse_{parent_cid}"
                        browse_cache.pop(cache_key, None)
                        log.info(f"因删除分享，清除了目录缓存: {cache_key}")
                    else:
                        # 分享自根目录
                        root_cid = APP_CONFIG.get('root_cid', '0')
                        cache_key = f"browse_{root_cid}"
                        browse_cache.pop(cache_key, None)
                        log.info(f"因删除分享，清除了根目录缓存: {cache_key}")
                except (json.JSONDecodeError, IndexError, TypeError) as e:
                    log.warning(f"删除分享时解析路径失败，无法清除缓存: {e}")

                # 3. 现在，删除数据库中的条目
                await session.delete(entry_to_delete)
                
    return jsonify({"success": True})

@app.route('/api/manual-audit', methods=['POST'])
async def api_manual_audit():
    try:
        log.info("手动触发审核任务。")
        await check_share_status_task()
        return jsonify({"success": True, "message": "审核任务执行完成。"})
    except Exception as e:
        log.error("手动审核任务执行失败", exc_info=True)
        return jsonify({"error": f"内部错误: {e}"}), 500

@app.route('/api/manual-clean', methods=['POST'])
async def api_manual_clean():
    try:
        log.info("手动触发清理任务。")
        await master_cleanup_task()
        return jsonify({"success": True, "message": "清理任务执行完成。"})
    except Exception as e:
        log.error("手动清理任务执行失败", exc_info=True)
        return jsonify({"error": f"内部错误: {e}"}), 500

@app.route('/api/check-share', methods=['POST'])
async def api_check_share():
    data = await request.get_json()
    share_url = data.get('share_url')
    async with task_lock:
        try:
            async with get_session() as session:
                entry = await session.get(Shares, share_url)
                if not entry:
                    return jsonify({"error": "未找到该分享"}), 404
                success, message = await _check_single_share_status(entry)
                if success:
                    return jsonify({"success": True, "message": "状态检查成功！"})
                else:
                    return jsonify({"error": f"检查失败: {message}"}), 500
        except Exception as e:
            log.error(f"手动检查数据库操作失败", exc_info=True)
            return jsonify({"error": f"数据库操作失败: {e}"}), 500

@app.route('/api/clean-share', methods=['POST'])
async def api_clean_share():
    data = await request.get_json()
    share_url = data.get('share_url')
    if not p115_client:
        return jsonify({"error": "115客户端未初始化"}), 500
    async with task_lock:
        try:
            async with get_session() as session:
                entry = await session.get(Shares, share_url)
                if not entry:
                    return jsonify({"error": "未找到该分享"}), 404
                
                if entry.status not in [4, 6]:
                    return jsonify({"error": "只有'已入库'或'清理失败'状态的分享才能被清理"}), 400

                file_ids = json.loads(entry.shared_cids) if entry.shared_cids else None
                if not file_ids:
                    entry.status = 5
                    log.info(f"记录 '{entry.share_title}' 无文件ID，直接标记为完成。")
                    return jsonify({"success": True, "message": "无文件ID，已标记为完成"})

                delete_payload = {"file_ids": ",".join(map(str, file_ids))}
                delete_response = await asyncio.to_thread(p115_client.fs_delete_app, delete_payload)

                if delete_response.get("state"):
                    log.info(f"✅ 文件已手动删除/重试删除: {entry.share_title}")
                    entry.status = 5
                    return jsonify({"success": True, "message": "文件清理成功！"})
                else:
                    log.warning(f"❗ 文件手动删除/重试删除失败: {entry.share_title} - 原因: {delete_response.get('error', '未知错误')}")
                    entry.status = 6
                    return jsonify({"error": f"115 API删除失败: {delete_response.get('error', '未知错误')}"}), 500
        except Exception as e:
            log.error(f"手动清理失败", exc_info=True)
            return jsonify({"error": f"内部错误: {e}"}), 500

@app.route('/api/update-share-status', methods=['POST'])
async def api_update_share_status():
    data = await request.get_json()
    share_url = data.get('share_url')
    new_status = data.get('status')
    if new_status is None:
        return jsonify({"error": "未提供新状态"}), 400
    async with task_lock:
        try:
            async with get_session() as session:
                update_stmt = update(Shares).where(Shares.share_url == share_url).values(status=int(new_status))
                result = await session.execute(update_stmt)
                if result.rowcount == 0:
                    return jsonify({"error": "未找到该分享或状态未改变"}), 404
                log.info(f"管理员手动将分享 {share_url} 的状态更新为 {new_status}")
                return jsonify({"success": True, "message": "状态更新成功！"})
        except Exception as e:
            log.error(f"手动更新状态失败", exc_info=True)
            return jsonify({"error": f"数据库操作失败: {e}"}), 500

@app.route('/api/transfer-share', methods=['POST'])
async def api_transfer_share():
    if not p115_client:
        return jsonify({"error": "115客户端未初始化"}), 500
    
    data = await request.get_json()
    original_link = data.get('shareLink')
    category_name = data.get('category')
    if not original_link or not category_name:
        return jsonify({"error": "分享链接和类型不能为空"}), 400

    log.info(f"开始转存分享: {original_link}")
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
        
        log.info(f"转存成功，新分享为: {new_share.share_title}")
        result_data = {c.name: getattr(new_share, c.name) for c in new_share.__table__.columns if c.name not in ['_sa_instance_state']}
        return jsonify({"success": True, "data": result_data})
        
    except Exception as e:
        log.error(f"转存分享链接 '{original_link}' 失败", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if transferred_file_ids:
            try:
                await asyncio.sleep(5) 
                delete_payload = {"file_ids": ",".join(transferred_file_ids)}
                await asyncio.to_thread(p115_client.fs_delete_app, delete_payload)
                log.info(f"临时文件 {transferred_file_ids} 清理完毕。")
            except Exception as e:
                log.error(f"清理临时文件 {transferred_file_ids} 失败: {e}")

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