FROM python:3.12-slim

WORKDIR /code

COPY services/payment_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY services/payment_service/app ./app

ENV PYTHONPATH=/code

EXPOSE 8003

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8003"]
