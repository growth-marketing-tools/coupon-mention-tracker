FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md ./

RUN uv sync --frozen --no-cache --no-dev --no-install-project

COPY src/ src/

RUN uv pip install --system --no-cache .

RUN useradd --create-home appuser
USER appuser

CMD ["python", "-m", "coupon_mention_tracker"]
