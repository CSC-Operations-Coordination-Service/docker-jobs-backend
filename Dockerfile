# syntax=docker/dockerfile:1
#
# docker-jobs-backend: installs the wheel `tox -e build` produces locally, or
# the python-ci workflow hands over as the `dist` artifact. The backend is an
# application: its wheel is never published, the image is the release.
#
# Build locally (drop the netrc secret if you have no ~/.netrc):
#   tox -e build
#   DOCKER_CONTEXT=default docker build -t docker-jobs-backend:dev \
#     --secret id=pip_index_url,env=PIP_INDEX_URL \
#     --secret id=netrc,src=$HOME/.netrc .
#
ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim AS runtime
LABEL Author="Serco - Space unit"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies come from the Nexus PyPI index when the pip_index_url secret is
# given (CI: mount-pip-index), from pip's default index otherwise.
COPY dist/ /tmp/dist/
RUN --mount=type=secret,id=pip_index_url \
    --mount=type=secret,id=netrc,target=/root/.netrc \
    set -eu; \
    if [ -s /run/secrets/pip_index_url ]; then \
        index_url="$(tr -d '\r\n' < /run/secrets/pip_index_url)"; \
        hostpart="${index_url#*://}"; hostpart="${hostpart%%/*}"; \
        index_host="${hostpart##*@}"; index_host="${index_host%%:*}"; \
        echo "Nexus index host: $index_host"; \
        PIP_INDEX_URL="$index_url"; PIP_TRUSTED_HOST="$index_host"; \
        export PIP_INDEX_URL PIP_TRUSTED_HOST; \
    fi; \
    pip install --no-cache-dir --no-input /tmp/dist/*.whl; \
    rm -rf /tmp/dist

# uid/gid 10001 like every house image. /data holds the SQLite file and the
# scheduler lock; owned by appuser, so a fresh named volume starts writable.
RUN groupadd -g 10001 appuser \
    && useradd -u 10001 -g appuser -M -s /usr/sbin/nologin appuser \
    && mkdir /data && chown appuser:appuser /data

USER appuser
VOLUME ["/data"]

ENV PORT=8000

EXPOSE 8000

# python rather than curl: the slim image ships no curl
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD python -c 'import os, sys, urllib.request; sys.exit(0 if urllib.request.urlopen("http://127.0.0.1:" + os.environ["PORT"] + "/health", timeout=5).status == 200 else 1)'

# ONE process, never --workers: the scheduler thread must be alone (LOCK_FILE)
CMD ["sh", "-c", "exec uvicorn --factory docker_jobs_backend.main:create_app --host 0.0.0.0 --port ${PORT} --proxy-headers --forwarded-allow-ips='*'"]
