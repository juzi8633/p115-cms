# app/__init__.py
import sys
import asyncio
import httpx
import traceback
from quart import Quart
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from p115client import P115Client
from .config import load_config, DEFAULT_CONFIG

# --- 日志重定向 ---
class Logger:
    def __init__(self, filename="app.log"):
        self.terminal = sys.stdout
        self.log = open(filename, "a", encoding='utf-8')
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.flush()
    def flush(self):
        self.terminal.flush()
        self.log.flush()
sys.stdout = Logger()
sys.stderr = sys.stdout

# --- 全局变量与客户端初始化 ---
scheduler = AsyncIOScheduler()
http_client = httpx.AsyncClient(timeout=20, follow_redirects=True)
task_lock = asyncio.Lock()

app = Quart(__name__, template_folder='../templates')

# 加载配置
APP_CONFIG = DEFAULT_CONFIG.copy()
load_config(APP_CONFIG)
app.config.from_mapping(APP_CONFIG)

# 初始化 115 客户端
p115_client = None
try:
    with open(APP_CONFIG['COOKIES_FILE'], "r", encoding="utf-8-sig") as f:
        cookies_content = f.read().strip()
    p115_client = P115Client(cookies_content)
    print("[INFO] ✅ 115 客户端初始化并验证成功。")
except Exception as e:
    print(f"[FATAL] ❌ 致命错误: 客户端初始化失败: {e}。")
    traceback.print_exc()

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
        print("[INFO] 🚀 服务启动于 http://127.0.0.1:5000")
        print("[INFO] 📢 Emby Webhook 监听于 http://127.0.0.1:5000/emby-webhook?token=YOUR_TOKEN")
    else:
        print("[INFO] 🛑 服务无法启动，请检查上方显示的致命错误信息并修复。")

@app.after_serving
async def shutdown():
    """应用关闭后执行"""
    if scheduler.running:
        scheduler.shutdown()
    await http_client.aclose()
    print("\n[INFO] 🛑 服务已关闭。")