FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir --no-build-isolation -r requirements.txt

CMD ["python3","app.py"]
