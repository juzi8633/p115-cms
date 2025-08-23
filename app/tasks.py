# app/tasks.py
import time
import json
import os
import traceback
import asyncio
import logging
from datetime import datetime
from pathlib import Path
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
        folder_names = [item.get("name") for item in path_hierarchy if item.get("name")]
        base_path = APP_CONFIG.get('cms_sync_path', '/media/share/')
        p = Path(base_path)
        for name in folder_names:
            p = p / name
        local_path = p.as_posix()
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

async def _trigger_cms_lift_sync():
    """根据配置触发CMS的增量同步接口"""
    cms_domain = APP_CONFIG.get("cms_domain")
    cms_token = APP_CONFIG.get("cms_token")

    if not (cms_domain and cms_token):
        log.info("CMS 域名或 Token 未配置，跳过最终的增量同步。")
        return

    # 构造请求 URL 和 Headers
    sync_url = f"{cms_domain.rstrip('/')}/api/sync/lift"
    headers = {"Authorization": f"Bearer {cms_token}"}
    
    log.info(f"正在触发 CMS 增量同步接口: {sync_url}")
    try:
        # 根据您的curl命令，这是一个GET请求
        response = await http_client.get(sync_url, headers=headers, timeout=60) # 延长超时时间以备同步耗时
        response.raise_for_status()
        log.info(f"✅ CMS 增量同步请求已成功发送, 响应: {response.text}")
    except Exception as e:
        log.error(f"❌ 发送 CMS 增量同步请求失败: {e}", exc_info=True)

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

async def delete_synced_files_task_unit() -> bool:
    log.info("🧹 开始执行一个清理批次...")
    BATCH_SIZE = int(APP_CONFIG.get("delete_task_batch_size", 10))
    # 获取API分块大小
    CHUNK_SIZE = int(APP_CONFIG.get("delete_api_chunk_size", 230))

    async with get_session() as session:
        # 1. 一次性获取批次内所有符合条件的 Shares 对象
        query = select(Shares).filter(Shares.status.in_([4, 6])).limit(BATCH_SIZE)
        result = await session.execute(query)
        entries_to_process = result.scalars().all()

        if not entries_to_process:
            log.info("✅ 没有待清理记录。")
            return True

        log.info(f"本批次处理 {len(entries_to_process)} 个待清理项。")

        # 2. 聚合所有文件ID，并按情况分类
        all_file_ids_to_delete = []
        urls_with_files = []
        urls_without_files = []

        for entry in entries_to_process:
            try:
                file_ids = json.loads(entry.shared_cids) if entry.shared_cids else None
                if file_ids:
                    all_file_ids_to_delete.extend(file_ids)
                    urls_with_files.append(entry.share_url)
                else:
                    urls_without_files.append(entry.share_url)
            except (json.JSONDecodeError, TypeError):
                log.warning(f"解析分享 '{entry.share_title}' 的 cids 失败，跳过此记录。")
                continue
        
        # 3. 批量更新没有文件ID的记录状态
        if urls_without_files:
            update_stmt_no_files = update(Shares).where(
                Shares.share_url.in_(urls_without_files)
            ).values(status=5)
            await session.execute(update_stmt_no_files)
            log.info(f"{len(urls_without_files)} 个无文件ID的记录已直接标记为完成。")

        # 4. 如果有文件需要删除，则分块执行API调用
        if all_file_ids_to_delete:
            any_chunk_failed = False
            unique_file_ids = list(set(all_file_ids_to_delete)) # 去重，避免重复删除
            log.info(f"聚合到 {len(unique_file_ids)} 个唯一文件ID需要删除。")

            # 将总列表切割成多个大小为 CHUNK_SIZE 的子列表
            for i in range(0, len(unique_file_ids), CHUNK_SIZE):
                chunk = unique_file_ids[i:i + CHUNK_SIZE]
                log.debug(f"正在处理文件ID块 {i//CHUNK_SIZE + 1}，包含 {len(chunk)} 个文件。")
                
                delete_payload = {"file_ids": ",".join(map(str, chunk))}
                try:
                    delete_response = await asyncio.to_thread(p115_client.fs_delete_app, delete_payload)
                    if not delete_response.get("state"):
                        log.warning(f"❗ 块删除失败: {delete_response.get('error', '未知错误')}")
                        any_chunk_failed = True
                        break # 一旦有块失败，立即中断后续删除操作
                except Exception as e:
                    log.error(f"❗ 块删除时发生API请求异常", exc_info=True)
                    any_chunk_failed = True
                    break

            # 5. 根据所有块的执行结果，统一更新数据库状态
            if any_chunk_failed:
                new_status = 6  # 失败 -> 清理失败
                log.warning(f"由于至少一个API请求块失败，本批次 {len(urls_with_files)} 个分享均标记为清理失败。")
            else:
                new_status = 5  # 成功 -> 已完成
                log.info(f"✅ 所有文件块均已成功删除。")
            
            update_stmt_with_files = update(Shares).where(
                Shares.share_url.in_(urls_with_files)
            ).values(status=new_status)
            await session.execute(update_stmt_with_files)

    # 6. 返回是否所有任务都已处理完毕
    return len(entries_to_process) < BATCH_SIZE


async def master_cleanup_task():
    log.info(f"【任务开始】总清理任务启动...")
    if not p115_client: return
    
    async with task_lock:
        try:
            BATCH_INTERVAL_MINUTES = int(APP_CONFIG.get("delete_task_batch_interval_minutes", 5))
            
            log.info("执行首个清理批次...")
            all_done = await delete_synced_files_task_unit()

            while not all_done:
                log.debug(f"批次间休眠 {BATCH_INTERVAL_MINUTES} 分钟...")
                await asyncio.sleep(BATCH_INTERVAL_MINUTES * 60)
                
                log.info("执行下一个清理批次...")
                all_done = await delete_synced_files_task_unit()
            
            log.info("所有清理批次均已完成。")
            log.info("开始执行最终的 Emby 刷新。")
            await _trigger_cms_lift_sync()
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
