#!/bin/bash
set -e

echo "=== SLAMD Client Starting ==="
echo "Waiting for SLAMD Server (slamd-server:3000)..."

# 서버의 Client Listener 포트가 열릴 때까지 대기
until timeout 2 bash -c "echo > /dev/tcp/slamd-server/3000" 2>/dev/null; do
  echo "SLAMD Server is not ready - sleeping 3s..."
  sleep 3
done
echo "SLAMD Server is ready."

# set-java-home.sh가 JAVA_HOME을 찾으려 하므로 미리 설정
export JAVA_HOME="${JAVA_HOME:-/opt/java/openjdk}"

# 클라이언트 실행 (foreground)
exec java -server -Xms512m -Xmx512m \
    -cp "lib/*:classes" \
    com.slamd.tools.CommandLineClient \
    -f /opt/slamd-client/slamd-client.conf \
    -c /opt/slamd-client/classes