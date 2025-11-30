FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy .env (optional)
COPY .env ./

# Command to run your mounted script
CMD ["streamlit", "run", "/app/scripts/streamlit_test.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
