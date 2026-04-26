# ---- 构建阶段 ----
FROM python:3.9-slim AS builder
WORKDIR /app

# 先复制依赖文件
COPY requirements.txt .

# 安装依赖到指定目录
RUN pip install --no-cache-dir --target=/install -r requirements.txt

# ---- 运行阶段 ----
FROM python:3.9-slim
WORKDIR /app

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    apt-get update && \
    apt-get install -y --no-install-recommends tzdata && \
    rm -rf /var/lib/apt/lists/*

# 从构建阶段复制依赖
COPY --from=builder /install /usr/local/lib/python3.9/site-packages/

# 复制应用代码
COPY main.py config.yaml ./

# 创建非 root 用户运行（安全最佳实践）
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python", "main.py"]
