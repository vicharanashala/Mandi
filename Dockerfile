# 🔁 Change python version if needed
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Install curl (not included in slim image, needed by agmarknet scraper)
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Install Playwright with Chromium and all required system dependencies
RUN python -m playwright install --with-deps chromium
# Copy all your project files
COPY . .

# EXPOSE 8000 intentionally removed — this is a Cloud Run Job, not a service.

# Default command for Cloud Run Job / Docker Hub deploy.
# check_db.py is NOT run here — run it manually when needed.
# Override CMD in Cloud Run Console only if you need different behavior.
CMD ["python", "main.py"]
