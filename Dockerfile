FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY app.py /app/
COPY us_visa /app/us_visa

CMD ["python3","app.py"]
