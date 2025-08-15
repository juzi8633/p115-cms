# run.py
from app import app

if __name__ == "__main__":
    # 使用Quart内置的run方法，它默认使用高性能的Hypercorn服务器
    app.run(host="0.0.0.0", port=5000)