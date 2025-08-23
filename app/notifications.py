# app/notifications.py
import re
import logging
from datetime import datetime
import pytz
from typing import Dict, Any
from app import http_client, APP_CONFIG

log = logging.getLogger(__name__)

async def send_feishu_card_notification(title: str, text_content: str, color: str = "blue"):
    """发送一个通用的飞书卡片通知。"""
    feishu_url = APP_CONFIG.get("feishu_webhook_url")
    if not feishu_url:
        return

    payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"title": {"tag": "plain_text", "content": title}, "template": color},
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": text_content}}]
        }
    }
    try:
        await http_client.post(feishu_url, json=payload, timeout=10)
    except Exception as e:
        log.error(f"发送通用飞书通知失败", exc_info=True)

async def send_feishu_notification_from_emby(payload: Dict[str, Any]):
    """处理 Emby webhook payload 并发送飞书通知。"""
    feishu_url = APP_CONFIG.get("feishu_webhook_url")
    if not feishu_url:
        log.warning("未配置飞书 Webhook URL，跳过 Emby 通知。")
        return

    event_type = payload.get("Event", "unknown_event")
    item_data = payload.get("Item", {})
    
    card_title = "【Emby】媒体通知"
    template_color = "blue"
    card_elements = []

    if event_type == "library.new":
        template_color = "green"
        item_type = item_data.get("Type")
        overview = item_data.get("Overview", "暂无简介")
        if overview and len(overview) > 100:
            overview = overview[:100] + "..."
            
        date_str = payload.get("Date", "")
        update_time_text = ""
        if date_str:
            try:
                utc_time = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                local_tz = pytz.timezone('Asia/Shanghai')
                local_time = utc_time.astimezone(local_tz)
                update_time_text = local_time.strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, pytz.UnknownTimeZoneError):
                pass

        plain_text_title = ""
        if item_type == "Episode":
            series_name = item_data.get("SeriesName", "未知剧集")
            season_num = item_data.get("ParentIndexNumber", 0)
            episode_num = item_data.get("IndexNumber", 0)
            episode_format = f"S{season_num:02d}E{episode_num:02d}"
            plain_text_title = f"{series_name} {episode_format}"
        elif item_type == "Series":
            series_name = item_data.get("Name", "未知项目")
            description = payload.get("Description", "")
            episode_range = description.split('\n')[0].strip() if description else ""
            plain_text_title = f"{series_name} {episode_range}"
        else: # Movie or other types
            item_name = item_data.get("Name", "未知项目")
            plain_text_title = item_name

        card_title = f"{plain_text_title} 入库成功"
        
        # --- 【核心修改】构建卡片元素时，不再添加重复的标题行 ---
        
        # 如果有更新时间，则用 note 元素显示
        if update_time_text:
            card_elements.append({"tag": "note", "elements": [{"tag": "plain_text", "content": f"更新于: {update_time_text}"}]})

        card_elements.append({"tag": "hr"})
        card_elements.append({"tag": "div", "text": {"tag": "lark_md", "content": overview}})

    elif event_type in ["library.deleted"]:
        template_color = "red"
        delete_description = payload.get("Title", "媒体已删除")
        card_title = delete_description
        user_data = payload.get("User", {})
        if event_type == "deep.delete" and (user_name := user_data.get("Name")):
            delete_description += f"\n\n操作用户: **{user_name}**"
        card_elements = [{"tag": "div", "text": {"tag": "lark_md", "content": delete_description}}]
    else:
        return

    # 添加 Path 字段
    path = item_data.get("Path")
    if path:
        path_parts = path.split('/')
        if len(path_parts) > 4:
            display_path = "/".join(path_parts[4:])
        else:
            display_path = path
        if card_elements: card_elements.append({"tag": "hr"})
        card_elements.append({"tag": "note", "elements": [{"tag": "plain_text", "content": f"路径: {display_path}"}]})

    # --- 发送飞书消息 (保持不变) ---
    feishu_payload = {"msg_type": "interactive","card": {"config": {"wide_screen_mode": True},"header": {"title": {"tag": "plain_text", "content": card_title}, "template": template_color},"elements": card_elements}}
    try:
        await http_client.post(feishu_url, json=feishu_payload, timeout=10)
    except Exception as e:
        log.error(f"发送 Emby Webhook 飞书通知失败", exc_info=True)