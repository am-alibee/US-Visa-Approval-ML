FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN echo "------- This is the new docker file -----------"

RUN ls -la

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3","app.py"]
