# Dockerfile

# 1. 使用官方的Python 3.11 slim版本作为基础镜像
FROM python:3.11-slim

# 2. 设置容器内的工作目录
WORKDIR /app

# 3. 复制依赖文件到工作目录
# (将这步和下一步分开，可以利用Docker的层缓存机制，在依赖不变时加快构建速度)
COPY requirements.txt .

# 4. 安装所有Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 5. 复制整个项目的代码到工作目录
COPY . .

# 6. 声明容器将监听的端口
EXPOSE 5000

# 7. 定义启动容器时执行的命令
CMD ["python", "run.py"]