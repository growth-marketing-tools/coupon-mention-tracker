FROM python:3.12-slim

WORKDIR /app

# Install uv for faster dependency installation
RUN pip install --no-cache-dir uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies using uv sync for reproducibility
# Use --frozen to ensure we use the exact versions in uv.lock
RUN uv sync --frozen --no-cache --no-dev --no-install-project

# Copy source code
COPY src/ src/

# Install the project itself
RUN uv pip install --system --no-cache .

# Run as non-root user
RUN useradd --create-home appuser
USER appuser

CMD ["python", "-m", "coupon_mention_tracker"]
