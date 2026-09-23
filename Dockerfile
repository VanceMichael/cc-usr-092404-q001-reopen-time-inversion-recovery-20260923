# 机场中断影响服务镜像。
# 应用仅使用 Python 3.12 标准库，构建时除基础镜像和 tzdata 外不下载依赖。
FROM python:3.12-alpine

# tzdata 提供跨午夜规则所需的 IANA 时区，BusyBox wget 用于健康检查。
RUN apk add --no-cache tzdata

WORKDIR /srv

COPY app/ ./app/
COPY fixtures/ ./fixtures/
COPY tests/ ./tests/
COPY scripts/selftest_client.py ./scripts/selftest_client.py

RUN addgroup -S app && adduser -S -G app -h /srv app \
    && mkdir -p /data \
    && chown -R app:app /srv /data

USER app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOST=0.0.0.0 \
    PORT=8080 \
    DB_PATH=/data/disruptions.db \
    FIXTURES_DIR=/srv/fixtures

VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=3s --timeout=3s --start-period=2s --retries=10 \
    CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1

CMD ["python", "-m", "app"]
