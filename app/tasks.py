# app/tasks.py
import time
import json
import traceback
import asyncio
import logging
from datetime import datetime
from sqlalchemy import func, select, update
from app import scheduler, p115_client, APP_CONFIG, task_lock, http_client, app
from app.notifications import send_feishu_card_notification
from app.database import get_session, Shares

log = logging.getLogger(__name__)

STATUS_TEXT_MAP = {0: "审核中", 1: "正常", 2: "违规", 3: "已失效", 4: "已入库", 5: "已完成", 6: "清理失败"}

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
        log.info("Emby 未配置，跳过媒体库刷新。")
        return

    url = f"{emby_domain.rstrip('/')}/Library/Refresh"
    headers = {"X-Emby-Token": api_key, "Content-Type": "application/json"}
    
    log.info("正在触发 Emby 媒体库刷新...")
    try:
        response = await http_client.post(url, headers=headers, timeout=15)
        response.raise_for_status()
        log.info("✅ Emby 媒体库刷新请求已成功发送。")
    except Exception as e:
        log.error(f"❌ 发送 Emby 刷新请求失败: {e}", exc_info=True)
        raise e

# --- 核心任务函数 ---
async def check_share_status_task():
    if not p115_client: return

    async with task_lock:
        log.info("【任务开始】分享状态检查...")
        BATCH_SIZE = int(APP_CONFIG.get("task_batch_size", 5))
        API_SLEEP_INTERVAL = int(APP_CONFIG.get("task_api_sleep_seconds", 15))

        try:
            async with get_session() as session:
                query = select(Shares).filter(Shares.status.in_([0, 1])).limit(BATCH_SIZE)
                result = await session.execute(query)
                entries_to_process = [{c.name: getattr(row[0], c.name) for c in row[0].__table__.columns} for row in result.fetchall()]
        except Exception as e:
            log.error(f"获取待处理任务批次时发生数据库错误", exc_info=True)
            return

        if not entries_to_process:
            log.info("没有需要检查或同步的分享记录。")
            log.info("【任务结束】分享状态检查完成。")
            return

        log.info(f"本次需处理 {len(entries_to_process)} 个分享。")

        for i, entry_data in enumerate(entries_to_process):
            try:
                current_status = entry_data['status']
                new_status = current_status

                if str(current_status) != '1':
                    log.debug(f"正在检查分享: {entry_data.get('share_title', 'N/A')}")
                    info_data = await asyncio.to_thread(p115_client.share_info_app, {"share_code": entry_data["share_code"]})
                    if info_data.get("state"):
                        new_status = info_data.get("data", {}).get("share_state")
                    elif "已取消" in info_data.get('error', ''):
                        new_status = 3
                
                if str(new_status) == '1':
                    log.debug(f"正在同步到CMS: {entry_data.get('share_title', 'N/A')}")
                    success, message = await _sync_to_cms(entry_data)
                    if success:
                        new_status = 4
                    else:
                        log.warning(f"同步失败: {message}")
                
                if new_status != current_status:
                    async with get_session() as session:
                        update_stmt = update(Shares).where(Shares.share_url == entry_data['share_url']).values(status=new_status)
                        await session.execute(update_stmt)
                    current_status_text = STATUS_TEXT_MAP.get(current_status, '未知')
                    new_status_text = STATUS_TEXT_MAP.get(new_status, '未知')
                    log.info(f"分享状态变更: '{entry_data.get('share_title')}' 从 [{current_status_text}] -> [{new_status_text}]")

                if i < len(entries_to_process) - 1:
                    log.debug(f"API 调用间隔休眠 {API_SLEEP_INTERVAL} 秒...")
                    await asyncio.sleep(API_SLEEP_INTERVAL)

            except Exception as e:
                log.error(f"处理分享 {entry_data.get('share_url')} 时失败", exc_info=True)
                await send_feishu_card_notification(
                    "【❌ 单项任务失败】115 智能管理器告警",
                    f"处理分享`{entry_data.get('share_title')}`时发生错误:\n```{e}```",
                    "orange"
                )
                continue
        log.info("【任务结束】分享状态检查完成。")

# ↓↓↓ 已重构此函数以确保事务原子性 ↓↓↓
async def delete_synced_files_task_unit() -> bool:
    log.info("🧹 开始执行一个清理批次...")
    BATCH_SIZE = int(APP_CONFIG.get("delete_task_batch_size", 10))
    SLEEP_INTERVAL = int(APP_CONFIG.get("delete_task_sleep_seconds", 30))

    share_urls_to_process = []
    async with get_session() as session:
        query = select(Shares.share_url).filter_by(status=4).limit(BATCH_SIZE)
        result = await session.execute(query)
        share_urls_to_process = result.scalars().all()

    if not share_urls_to_process:
        log.info("✅ 没有待清理记录。")
        return True

    log.info(f"本批次处理 {len(share_urls_to_process)} 个待清理项。")

    for i, share_url in enumerate(share_urls_to_process):
        try:
            async with get_session() as session:
                query = select(Shares).filter_by(share_url=share_url)
                result = await session.execute(query)
                entry = result.scalar_one_or_none()

                if not entry:
                    log.warning(f"记录 {share_url} 在处理时未找到，可能已被其他进程处理。")
                    continue

                file_ids = json.loads(entry.shared_cids) if entry.shared_cids else None
                
                if not file_ids:
                    entry.status = 5
                    log.info(f"记录 '{entry.share_title}' 无文件ID，直接标记为完成。")
                    continue

                delete_payload = ",".join(map(str, file_ids))
                delete_response = await asyncio.to_thread(p115_client.fs_delete, delete_payload)
                
                if delete_response.get("state"):
                    log.info(f"✅ 文件已删除: {entry.share_title}")
                    entry.status = 5
                else:
                    log.warning(f"❗ 文件删除失败: {entry.share_title} - 原因: {delete_response.get('error', '未知错误')}")
                    entry.status = 6
        
        except Exception as e:
            log.error(f"处理分享 {share_url} 时发生意外数据库或IO错误", exc_info=True)

        if i < len(share_urls_to_process) - 1:
            log.debug(f"文件处理间隔休眠 {SLEEP_INTERVAL} 秒...")
            await asyncio.sleep(SLEEP_INTERVAL)
            
    return len(share_urls_to_process) < BATCH_SIZE

async def master_cleanup_task():
    log.info(f"【任务开始】总清理任务启动...")
    if not p115_client: return
    
    async with task_lock:
        try:
            BATCH_INTERVAL_MINUTES = int(APP_CONFIG.get("delete_task_batch_interval_minutes", 5))
            while True:
                all_done = await delete_synced_files_task_unit()
                if all_done:
                    log.info("所有清理批次均已完成。")
                    break
                
                log.debug(f"批次间休眠 {BATCH_INTERVAL_MINUTES} 分钟...")
                await asyncio.sleep(BATCH_INTERVAL_MINUTES * 60)
            
            log.info("开始执行最终的 Emby 刷新。")
            await _refresh_emby_library()
            log.info("【任务结束】总清理任务执行完毕。")

        except Exception as e:
            log.error(f"总清理任务执行失败", exc_info=True)
            error_msg = (
                f"**失败模块**: 总清理任务\n"
                f"**失败时间**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"**错误详情**: ```\n{traceback.format_exc()}\n```"
            )
            await send_feishu_card_notification("【❌ 任务失败】115 智能管理器告警", error_msg, "red")

async def send_daily_summary_task():
    log.info("【任务开始】生成并发送每日摘要...")
    status_counts = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}
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
        log.error(f"生成摘要时数据库查询失败", exc_info=True)
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
        f"- ❌ **清理失败**: {status_counts.get(6, 0)} 条\n"
    )
    await send_feishu_card_notification(f"【📊 每日报告】115 智能管理器", content, "blue")
    log.info("【任务结束】每日摘要已发送。")

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
            log.info(f"🕒 审核任务已启用，计划: {cron_str}")
        except Exception as e:
            log.error(f"无法设置审核任务，请检查 Cron 表达式 '{cron_str}': {e}")
    
    if APP_CONFIG.get("delete_task_enabled"):
        cron_str = APP_CONFIG.get('delete_task_cron', '0 4 * * *')
        try:
            cron_parts = cron_str.split()
            if len(cron_parts) != 5: raise ValueError("Cron 表达式必须有5个部分")
            scheduler.add_job(master_cleanup_task, 'cron', 
                              minute=cron_parts[0], hour=cron_parts[1], day=cron_parts[2], 
                              month=cron_parts[3], day_of_week=cron_parts[4], 
                              id='delete_synced_job')
            log.info(f"🧹 总清理任务已启用，计划: {cron_str}")
        except Exception as e:
            log.error(f"无法设置清理任务，请检查 Cron 表达式 '{cron_str}': {e}")
    
    if APP_CONFIG.get("daily_summary_enabled"):
        cron_str = APP_CONFIG.get('summary_task_cron', '55 23 * * *')
        try:
            cron_parts = cron_str.split()
            if len(cron_parts) != 5: raise ValueError("Cron 表达式必须有5个部分")
            scheduler.add_job(send_daily_summary_task, 'cron', 
                              minute=cron_parts[0], hour=cron_parts[1], day=cron_parts[2], 
                              month=cron_parts[3], day_of_week=cron_parts[4], 
                              id='daily_summary_job')
            log.info(f"📊 每日摘要任务已启用，计划: {cron_str}")
        except Exception as e:
            log.error(f"无法设置每日摘要任务，请检查 Cron 表达式 '{cron_str}': {e}")
