FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY webhook_receiver.py .

EXPOSE 5000

CMD ["python", "webhook_receiver.py"]
