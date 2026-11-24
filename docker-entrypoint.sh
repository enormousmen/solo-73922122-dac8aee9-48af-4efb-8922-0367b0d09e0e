#!/bin/sh
# 等待 PostgreSQL 就绪后初始化 schema，再启动 Flask。
set -e

echo "[entrypoint] 等待数据库 ${PGHOST}:${PGPORT} ..."
python - <<'PY'
from app import db
db.wait_for_database(timeout=60)
db.ensure_database()
db.init_schema(seed=True)
print("[entrypoint] 数据库初始化完成")
PY

exec python -m app
