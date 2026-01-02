FROM python:3.12-slim

WORKDIR /app

# Install uv for faster dependency installation
RUN pip install --no-cache-dir uv

# Copy dependency files
COPY pyproject.toml .

# Install dependencies
RUN uv pip install --system --no-cache .

# Copy source code
COPY src/ src/

# Run as non-root user
RUN useradd --create-home appuser
USER appuser

CMD ["python", "-m", "coupon_mention_tracker"]
