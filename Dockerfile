FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# 备份演练需要 pg_dump / psql；通过 PGDG 仓库安装与 db 镜像
# postgres:16 主版本一致的 postgresql-client-16。
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && install -d /usr/share/postgresql-common/pgdg \
 && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
 && chmod 644 /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
 && . /etc/os-release \
 && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] " \
        "https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends postgresql-client-16 \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && pg_dump --version && psql --version

WORKDIR /srv/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 已通过核对、可供下载的备份目录（compose 用 bind mount 挂到宿主机 ./backups）
RUN mkdir -p /srv/app/backups
ENV BACKUP_DIR=/srv/app/backups

EXPOSE 8000
ENTRYPOINT ["docker-entrypoint.sh"]
