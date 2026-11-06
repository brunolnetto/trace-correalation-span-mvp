FROM python:3.12-slim

RUN python -m pip install --no-cache-dir --disable-pip-version-check uv==0.10.0

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./

RUN uv sync --frozen --no-dev --no-install-project

COPY app ./app

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000 8001

CMD ["uvicorn", "app.order.main:app", "--host", "0.0.0.0", "--port", "8000"]
