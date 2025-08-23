# app/config.py
import json

CONFIG_FILE = 'data/config.json'
COOKIES_FILE = 'config/115-cookies.txt'
DATABASE_FILE = 'data/data.db'

DEFAULT_CONFIG = {
    "SQLALCHEMY_DATABASE_URI": f"sqlite+aiosqlite:///{DATABASE_FILE}",
    "COOKIES_FILE": COOKIES_FILE,
    "DATABASE_FILE": DATABASE_FILE,
    "root_cid": "0",
    "target_cid": "0",
    "task_enabled": False,
    "task_cron": "*/10 * * * *",
    "task_batch_size": 5,
    "task_api_sleep_seconds": 15,
    "delete_task_enabled": False,
    "delete_task_cron": "0 4 * * *",
    "delete_task_batch_size": 10,
    "delete_task_sleep_seconds": 30,
    "delete_task_batch_interval_minutes": 5,
    "delete_api_chunk_size": 230,
    "daily_summary_enabled": True,
    "summary_task_cron": "55 23 * * *",
    "cms_domain": "",
    "cms_username": "",
    "cms_password": "",
    "cms_token": "",
    "cms_sync_path": "/media/share/",
    "emby_domain": "",
    "emby_api_key": "",
    "feishu_webhook_url": "",
    "emby_webhook_token": "",
    # --- 新增日志配置 ---
    "log_level": "INFO",
    "log_file": "data/app.log",
    "log_max_bytes": 10485760,
    "log_backup_count": 5
}

def load_config(config_dict):
    """加载用户配置并更新到传入的配置字典中"""
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            user_config = json.load(f)
        config_dict.update(user_config)
    except (FileNotFoundError, json.JSONDecodeError):
        save_config(config_dict)

def save_config(config_dict):
    """将配置字典中的可配置项保存到文件"""
    exclude_keys = {k for k in config_dict if k.isupper()}
    config_to_save = {k: v for k, v in config_dict.items() if k not in exclude_keys}
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config_to_save, f, indent=4, ensure_ascii=False)