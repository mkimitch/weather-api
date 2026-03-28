FROM python:3.11-slim

WORKDIR /app

# For healthcheck curl (optional but handy)
RUN apt-get update \
	&& apt-get install -y --no-install-recommends curl \
	&& rm -rf /var/lib/apt/lists/*

# Install deps first (better layer caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY . /app

# Create cache dir inside image (will be bind-mounted in compose)
RUN mkdir -p /app/weather_cache

EXPOSE 8000

# Production-ish defaults (no --reload)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
