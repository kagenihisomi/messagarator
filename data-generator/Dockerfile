FROM python:3.9-slim

WORKDIR /app

RUN pip install kafka-python

COPY data_generator.py .

CMD ["python", "data_generator.py"]