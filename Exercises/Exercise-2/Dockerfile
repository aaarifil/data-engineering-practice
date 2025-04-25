FROM python:3.9-slim

WORKDIR /app
COPY . .

RUN pip install -r requirements.txt
RUN pip install beautifulsoup4

CMD ["python", "main.py"]
