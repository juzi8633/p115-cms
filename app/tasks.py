# app/tasks.py
import time
import json
import traceback
import asyncio
from datetime import datetime
from sqlalchemy import func, select
from app import scheduler, p115_client, APP_CONFIG, task_lock, http_client, app
from app.notifications import send_feishu_card_notification
from app.database import get_session, Shares

# --- 任务辅助函数 ---
async def _sync_to_cms(entry):
    cms_token = APP_CONFIG.get("cms_token")
    cms_domain = APP_CONFIG.get("cms_domain")
    if not (cms_token and cms_domain):
        return False, "未配置CMS"
    try:
        sync_url = f"{cms_domain.rstrip('/')}/api/sync/share115"
        headers = {'Authorization': f'Bearer {cms_token}', 'Content-Type': 'application/json'}
        path_hierarchy_json = entry.get("path_hierarchy", "[]")
        path_hierarchy = json.loads(path_hierarchy_json)
        last_folder_name = path_hierarchy[-1].get("name", "") if path_hierarchy else ""
        local_path = f"{APP_CONFIG.get('cms_sync_path', '/media/share/')}{last_folder_name}"
        payload = {"share_code": entry.get("share_code"), "receive_code": entry.get("receive_code"), "cid": 0, "local_path": local_path}
        sync_response = await http_client.post(sync_url, json=payload, headers=headers)
        sync_response.raise_for_status()
        sync_data = sync_response.json()
        if sync_data.get("code") == 200:
            return True, "同步成功"
        else:
            return False, sync_data.get("msg", "未知CMS错误")
    except Exception as sync_e:
        return False, str(sync_e)

async def _refresh_emby_library():
    """根据配置触发 Emby 媒体库刷新"""
    emby_domain = APP_CONFIG.get("emby_domain")
    api_key = APP_CONFIG.get("emby_api_key")

    if not (emby_domain and api_key):
        print("[INFO] Emby 未配置，跳过媒体库刷新。")
        return

    url = f"{emby_domain.rstrip('/')}/Library/Refresh"
    headers = {"X-Emby-Token": api_key, "Content-Type": "application/json"}
    
    print("[EMBY] 触发 Emby 媒体库刷新...")
    try:
        response = await http_client.post(url, headers=headers, timeout=15)
        response.raise_for_status()
        print("[EMBY] ✅ Emby 媒体库刷新请求已成功发送。")
    except Exception as e:
        print(f"[EMBY] ❌ 发送 Emby 刷新请求失败: {e}")
        raise e

# --- 核心任务函数 ---
async def check_share_status_task():
    print(f"\n[TASK] 🕒 {time.strftime('%Y-%m-%d %H:%M:%S')} - 开始执行分享状态检查任务 (异步批处理)...")
    if not p115_client: return

    async with task_lock:
        try:
            BATCH_SIZE = int(APP_CONFIG.get("task_batch_size", 5))
            API_SLEEP_INTERVAL = int(APP_CONFIG.get("task_api_sleep_seconds", 15))

            async with get_session() as session:
                query = select(Shares).filter(Shares.status.in_([0, 1])).limit(BATCH_SIZE)
                result = await session.execute(query)
                entries_to_process = result.scalars().all()

                if not entries_to_process:
                    print("[TASK] ✅ 没有需要检查或同步的分享记录。")
                    return

                print(f"[TASK] 本次处理 {len(entries_to_process)} 个待处理项。")

                for entry in entries_to_process:
                    new_status = entry.status
                    if str(entry.status) != '1':
                        print(f"[TASK] 正在检查分享: {entry.share_title or 'N/A'}")
                        info_data = await asyncio.to_thread(p115_client.share_info_app, {"share_code": entry.share_code})
                        if info_data.get("state"):
                            new_status = info_data.get("data", {}).get("share_state")
                        elif "已取消" in info_data.get('error', ''):
                            new_status = 3
                    
                    if str(new_status) == '1':
                        print(f"[TASK] 正在同步到CMS: {entry.share_title or 'N/A'}")
                        entry_dict = {c.name: getattr(entry, c.name) for c in entry.__table__.columns}
                        success, message = await _sync_to_cms(entry_dict)
                        if success:
                            new_status = 4
                        else:
                            print(f"[TASK] ❗ 同步失败: {message}")
                    
                    if new_status != entry.status:
                        entry.status = new_status
                        print(f"[TASK] 💾 分享 {entry.share_title} 状态更新为 {new_status}。")

                    print(f"[TASK] ⏸️ API 调用间隔休眠 {API_SLEEP_INTERVAL} 秒...")
                    await asyncio.sleep(API_SLEEP_INTERVAL)
        except Exception as e:
            await db.session.rollback()
            full_traceback = traceback.format_exc()
            print(f"[TASK] ❌ 分享状态检查任务执行失败: {e}\n{full_traceback}")
            error_msg = (
                f"**失败模块**: 分享审核任务\n"
                f"**失败时间**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"**错误详情**: ```\n{full_traceback}\n```"
            )
            await send_feishu_card_notification("【❌ 任务失败】115 智能管理器告警", error_msg, "red")

async def delete_synced_files_task_unit() -> bool:
    print(f"[CLEANUP_UNIT] 🧹 开始执行一个清理批次...")
    BATCH_SIZE = int(APP_CONFIG.get("delete_task_batch_size", 10))
    SLEEP_INTERVAL = int(APP_CONFIG.get("delete_task_sleep_seconds", 30))

    async with get_session() as session:
        query = select(Shares).filter_by(status=4).limit(BATCH_SIZE)
        result = await session.execute(query)
        batch_to_delete = result.scalars().all()

        if not batch_to_delete:
            print("[CLEANUP_UNIT] ✅ 没有待清理记录。")
            return True

        print(f"[CLEANUP_UNIT] 本次处理 {len(batch_to_delete)} 个待清理项。")

        for entry in batch_to_delete:
            file_ids = json.loads(entry.shared_cids) if entry.shared_cids else None
            
            if not file_ids:
                entry.status = 5
                continue

            delete_payload = ",".join(map(str, file_ids))
            delete_response = await asyncio.to_thread(p115_client.fs_delete, delete_payload)
            
            if delete_response.get("state"):
                print(f"[CLEANUP_UNIT] ✅ 文件删除成功: {entry.share_title}")
                entry.status = 5
            else:
                print(f"[CLEANUP_UNIT] ❗ 文件删除失败: {delete_response.get('error', '未知错误')}")
            
            print(f"[CLEANUP_UNIT] ⏸️ 文件处理间隔休眠 {SLEEP_INTERVAL} 秒...")
            await asyncio.sleep(SLEEP_INTERVAL)
        
        count_query = select(func.count()).select_from(Shares).filter_by(status=4)
        result = await session.execute(count_query)
        return result.scalar_one() == 0

async def master_cleanup_task():
    print(f"\n[MASTER_CLEANUP] ︻╦╤─ {time.strftime('%Y-%m-%d %H:%M:%S')} - 总清理任务启动...")
    if not p115_client: return
    
    async with task_lock:
        try:
            BATCH_INTERVAL_MINUTES = int(APP_CONFIG.get("delete_task_batch_interval_minutes", 5))
            while True:
                all_done = await delete_synced_files_task_unit()
                if all_done:
                    print("[MASTER_CLEANUP] ✅ 所有清理批次均已完成。")
                    break
                
                print(f"[MASTER_CLEANUP] ⏸️ 批次间休眠 {BATCH_INTERVAL_MINUTES} 分钟...")
                await asyncio.sleep(BATCH_INTERVAL_MINUTES * 60)
            
            print("[MASTER_CLEANUP] 🎬 开始执行最终的 Emby 刷新。")
            await _refresh_emby_library()
            print("[MASTER_CLEANUP] 👍 总清理任务执行完毕。")

        except Exception as e:
            full_traceback = traceback.format_exc()
            print(f"[MASTER_CLEANUP] ❌ 总清理任务执行失败: {e}\n{full_traceback}")
            error_msg = (
                f"**失败模块**: 总清理任务\n"
                f"**失败时间**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"**错误详情**: ```\n{full_traceback}\n```"
            )
            await send_feishu_card_notification("【❌ 任务失败】115 智能管理器告警", error_msg, "red")

async def send_daily_summary_task():
    print(f"[SUMMARY_TASK] 💎 开始生成每日摘要...")
    status_counts = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    total_shares = 0
    try:
        async with get_session() as session:
            total_query = select(func.count()).select_from(Shares)
            total_result = await session.execute(total_query)
            total_shares = total_result.scalar_one()

            group_query = select(Shares.status, func.count(Shares.share_url)).group_by(Shares.status)
            result = await session.execute(group_query)
            for row in result.all():
                if row[0] in status_counts:
                    status_counts[row[0]] = row[1]
    except Exception as e:
        print(f"[SUMMARY_TASK] ❌ 生成摘要失败: {e}")
        return

    report_date = datetime.now().strftime('%Y-%m-%d')
    content = (
        f"**报告日期**: {report_date}\n\n---\n\n"
        f"**分享总览**\n- 📝 **总计**: {total_shares} 条\n\n"
        f"**状态分布**\n"
        f"- ⏳ **审核中**: {status_counts[0]} 条\n"
        f"- ✅ **正常**: {status_counts[1]} 条\n"
        f"- 📦 **已入库**: {status_counts[4]} 条\n"
        f"- 🎉 **已完成**: {status_counts[5]} 条\n"
        f"- 🚫 **违规/失效**: {status_counts[2] + status_counts[3]} 条\n"
    )
    await send_feishu_card_notification(f"【📊 每日报告】115 智能管理器", content, "blue")
    print(f"[SUMMARY_TASK] ✅ 每日摘要已发送。")

def schedule_task():
    if scheduler.get_job('share_check_job'): scheduler.remove_job('share_check_job')
    if scheduler.get_job('delete_synced_job'): scheduler.remove_job('delete_synced_job')
    if scheduler.get_job('daily_summary_job'): scheduler.remove_job('daily_summary_job')

    if APP_CONFIG.get("task_enabled"):
        cron_str = APP_CONFIG.get('task_cron', '*/10 * * * *')
        try:
            cron_parts = cron_str.split()
            if len(cron_parts) != 5: raise ValueError("Cron 表达式必须有5个部分")
            scheduler.add_job(check_share_status_task, 'cron', 
                              minute=cron_parts[0], hour=cron_parts[1], day=cron_parts[2], 
                              month=cron_parts[3], day_of_week=cron_parts[4], 
                              id='share_check_job')
            print(f"[INFO] 🕒 审核任务已启用，计划: {cron_str}")
        except Exception as e:
            print(f"[ERROR] ❌ 无法设置审核任务，请检查 Cron 表达式 '{cron_str}': {e}")
    
    if APP_CONFIG.get("delete_task_enabled"):
        cron_str = APP_CONFIG.get('delete_task_cron', '0 4 * * *')
        try:
            cron_parts = cron_str.split()
            if len(cron_parts) != 5: raise ValueError("Cron 表达式必须有5个部分")
            scheduler.add_job(master_cleanup_task, 'cron', 
                              minute=cron_parts[0], hour=cron_parts[1], day=cron_parts[2], 
                              month=cron_parts[3], day_of_week=cron_parts[4], 
                              id='delete_synced_job')
            print(f"[INFO] 🧹 总清理任务已启用，计划: {cron_str}")
        except Exception as e:
            print(f"[ERROR] ❌ 无法设置清理任务，请检查 Cron 表达式 '{cron_str}': {e}")
    
    if APP_CONFIG.get("daily_summary_enabled"):
        cron_str = APP_CONFIG.get('summary_task_cron', '55 23 * * *')
        try:
            cron_parts = cron_str.split()
            if len(cron_parts) != 5: raise ValueError("Cron 表达式必须有5个部分")
            scheduler.add_job(send_daily_summary_task, 'cron', 
                              minute=cron_parts[0], hour=cron_parts[1], day=cron_parts[2], 
                              month=cron_parts[3], day_of_week=cron_parts[4], 
                              id='daily_summary_job')
            print(f"[INFO] 📊 每日摘要任务已启用，计划: {cron_str}")
        except Exception as e:
            print(f"[ERROR] ❌ 无法设置每日摘要任务，请检查 Cron 表达式 '{cron_str}': {e}")