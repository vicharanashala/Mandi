# 🔁 Change python version if needed
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all your project files
COPY . .

EXPOSE 8000        

# Default command (can be overridden in workflow)
CMD ["python", "main.py"]