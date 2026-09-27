FROM python:3.14-slim

WORKDIR /app

COPY exercicio11.py .

ENTRYPOINT ["python3", "exercicio11.py"]

CMD ["--n-tasks", "100", "--chunk-size", "100000"]
