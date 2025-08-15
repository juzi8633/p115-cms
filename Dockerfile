# Dockerfile

# 1. 使用官方的Python 3.12 slim版本作为基础镜像
FROM python:3.12-slim

# 2. 设置容器内的工作目录
WORKDIR /app

# 3. 安装系统依赖 (虽然本次不是主要问题，但保留是个好习惯)
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# 4. 复制依赖文件到工作目录
COPY requirements.txt .

# 5. 安装所有Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 6. 复制整个项目的代码到工作目录
COPY . .

# 7. 声明容器将监听的端口
EXPOSE 5000

# 8. 定义启动容器时执行的命令 (建议使用 hypercorn)
CMD ["hypercorn", "--bind", "0.0.0.0:5000", "app:app"]