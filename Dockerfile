FROM python:3.12-slim

# Set working directory
WORKDIR /backend_project

# Copy only requirements first to leverage Docker cache
COPY requirements.txt .

# Upgrade pip and install dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY ./app ./app

# Set PYTHONPATH for imports
ENV PYTHONPATH=/backend_project

# Expose the port
EXPOSE 8005

# Production CMD without reload
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8005","--reload"]
