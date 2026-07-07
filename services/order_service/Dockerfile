FROM python:3.12-slim

WORKDIR /code

COPY services/order_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY services/order_service/app ./app

ENV PYTHONPATH=/code

EXPOSE 8001

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
