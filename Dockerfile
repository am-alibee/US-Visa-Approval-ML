FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --no-build-isolation -r requirements.txt

COPY . .

CMD ["python3","app.py"]
