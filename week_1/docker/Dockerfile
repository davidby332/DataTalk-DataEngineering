FROM python:3.9

RUN pip install pandas sqlalchemy argparse pyarrow fastparquet psycopg2

WORKDIR /app

# docker run -it test:pandasv1 2021-01-15
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]