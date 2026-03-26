#!/bin/bash
set -e

echo "=== SLAMD Server Starting ==="
echo "Waiting for PostgreSQL..."

# PostgreSQL이 준비될 때까지 대기
until java -cp "webapps/slamd/WEB-INF/lib/*" \
    org.postgresql.util.ConnectionFactoryFinder 2>/dev/null || \
    timeout 2 bash -c "echo > /dev/tcp/slamd-postgres/5432" 2>/dev/null; do
  echo "PostgreSQL is not ready - sleeping 2s..."
  sleep 2
done
echo "PostgreSQL is ready."

# CATALINA_OPTS 설정
export CATALINA_OPTS="-server -Xms512m -Xmx512m -Djava.awt.headless=true"

# Tomcat을 foreground로 실행 (컨테이너 유지)
exec bin/catalina.sh run