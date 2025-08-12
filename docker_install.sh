#!/bin/bash

set -e

echo "=== Docker 및 Docker Compose 설치 스크립트 ==="

# 1. 기존 Docker 제거
echo "[1/6] 기존 Docker 제거..."
sudo apt-get remove -y docker docker-engine docker.io containerd runc || true

# 2. 패키지 업데이트 및 의존성 설치
echo "[2/6] 패키지 업데이트 및 의존성 설치..."
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# 3. Docker 공식 GPG 키 추가
echo "[3/6] Docker GPG 키 추가..."
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# 4. Docker 리포지토리 등록
echo "[4/6] Docker 리포지토리 추가..."
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# 5. Docker Engine 설치
echo "[5/6] Docker 설치..."
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 6. 설치 확인
echo "[6/6] 설치 확인..."
docker --version
docker compose version

echo "✅ Docker 및 Docker Compose 설치 완료!"
