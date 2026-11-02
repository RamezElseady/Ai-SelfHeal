FROM python:3.11-slim

WORKDIR /app

# تثبيت الأدوات الأساسية وتثبيت الـ AWS CLI
RUN apt-get update && apt-get install -y curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# تثبيت متطلبات البايثون
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# نسخ الكود
COPY webhook_receiver.py .

EXPOSE 5000

CMD ["python", "webhook_receiver.py"]
