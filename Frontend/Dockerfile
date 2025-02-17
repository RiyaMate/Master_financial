# Use Python 3.9 as base
FROM python:3.9

# Set the working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libssl-dev \
    libffi-dev \
    openssl \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy environment variables
COPY .env ./

# Copy requirements.txt
COPY requirements.txt ./

# Upgrade pip before installing dependencies
RUN pip install --upgrade pip

# Downgrade NumPy to avoid compatibility issues
RUN pip install numpy==1.24.3 --force-reinstall

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Reinstall Snowflake Connector to ensure correct OpenSSL
RUN pip uninstall -y snowflake-connector-python && \
    pip install --no-cache-dir snowflake-connector-python

# Install certifi to avoid SSL issues
RUN pip install --upgrade certifi

# Copy the rest of your code
COPY . .

# Expose Streamlit port
EXPOSE 8501

# Default command to run Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
