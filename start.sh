#!/bin/bash
# 智能订单采集运营平台 — 一键启动脚本
set -e
cd "$(dirname "$0")"

echo "========================================="
echo "  智能订单采集运营平台"
echo "  Intelligent Order Collection Platform"
echo "========================================="

# 检查 Python 依赖
if ! python3 -c "import fastapi" 2>/dev/null; then
    echo "[1/3] 安装 Python 依赖..."
    pip3 install -r requirements.txt -q
else
    echo "[1/3] Python 依赖已就绪"
fi

# 构建前端（如果 dist 不存在）
if [ ! -d "frontend/dist" ]; then
    echo "[2/3] 构建前端..."
    cd frontend
    npm install --silent
    npm run build
    cd ..
else
    echo "[2/3] 前端已构建"
fi

echo "[3/3] 启动服务..."
echo ""
echo "  访问地址: http://localhost:8000"
echo "  API 文档: http://localhost:8000/docs"
echo ""
echo "  按 Ctrl+C 停止服务"
echo "========================================="
echo ""

python3 main.py
