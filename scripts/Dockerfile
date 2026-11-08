FROM python:3.12-slim

WORKDIR /app

# Copy the zero-dependency seeder & CDC simulator
COPY seed_mainframe_data.py /app/seed_mainframe_data.py

ENV SEED_ROWS=10000 \
    DB_PORT=5432

ENTRYPOINT ["python", "/app/seed_mainframe_data.py"]
CMD ["--action", "seed", "--rows", "10000"]
