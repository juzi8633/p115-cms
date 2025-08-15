# app/__init__.py
import sys
import asyncio
import httpx
import traceback
import logging
import os
from logging.handlers import RotatingFileHandler
from quart import Quart
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from p115client import P115Client
from .config import load_config, DEFAULT_CONFIG

# --- 日志配置 ---
def setup_logging(config):
    log_level_str = config.get("log_level", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    log_file = config.get("log_file", "data/app.log")
    max_bytes = int(config.get("log_max_bytes", 1024 * 1024 * 10)) # 默认 10MB
    backup_count = int(config.get("log_backup_count", 5))

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # ↓↓↓ 新增：优化第三方库的日志输出 ↓↓↓
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("hypercorn").setLevel(logging.WARNING)
    
    logging.info("日志系统配置完成。")

# --- 全局变量与客户端初始化 ---
scheduler = AsyncIOScheduler()
http_client = httpx.AsyncClient(timeout=20, follow_redirects=True)
task_lock = asyncio.Lock()

app = Quart(__name__, template_folder='../templates')

# 加载配置
APP_CONFIG = DEFAULT_CONFIG.copy()
load_config(APP_CONFIG)
setup_logging(APP_CONFIG)
log = logging.getLogger(__name__)
app.config.from_mapping(APP_CONFIG)

# 初始化 115 客户端
p115_client = None
try:
    with open(APP_CONFIG['COOKIES_FILE'], "r", encoding="utf-8-sig") as f:
        cookies_content = f.read().strip()
    p115_client = P115Client(cookies_content)
    log.info("✅ 115 客户端初始化成功。")
except Exception as e:
    log.critical(f"❌ 致命错误: 115 客户端初始化失败: {e}。", exc_info=True)

# --- 注册路由和定义生命周期函数 ---
from app import routes
from app.tasks import schedule_task

@app.before_serving
async def startup():
    """应用启动前执行"""
    from app.database import init_db
    await init_db()

    if p115_client:
        schedule_task()
        if not scheduler.running:
            scheduler.start()
        log.info("🚀 服务启动于 http://0.0.0.0:5000 (可被局域网访问)")
        log.info("📢 Emby Webhook 监听于 http://<本机局域网IP>:5000/emby-webhook?token=YOUR_TOKEN")
    else:
        log.warning("🛑 服务无法启动，请检查上方显示的致命错误信息并修复。")

@app.after_serving
async def shutdown():
    """应用关闭后执行"""
    if scheduler.running:
        scheduler.shutdown()
    await http_client.aclose()
    log.info("\n🛑 服务已关闭。")
