import pandas as  pd
from sqlalchemy import create_engine
import sysconfig

import argparse

# Parse Arguments

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database_name = params.db
    table_name = params.table_name
    url = params.url

    print(params)


    taxi_df = pd.read_parquet(
        url
    )

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{database_name}'
    )

    engine.connect()

    taxi_df.to_sql(
        name = table_name, 
        con = engine, 
        if_exists = 'replace'
    )

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Ingest CSV data to Postgres'
    )

    parser.add_argument(
        '--user',
        help = 'user name for postgres'
    )

    parser.add_argument(
        '--password',
        help = 'password for postgres'
    )

    parser.add_argument(
        '--host',
        help = 'password for postgres'
    )

    parser.add_argument(
        '--port',
        help = 'port for postgres'
    )

    parser.add_argument(
        '--db',
        help = 'database name for postgres'
    )

    parser.add_argument(
        '--table_name',
        help = 'table name for postgres'
    )

    parser.add_argument(
        '--url',
        help = 'url for taxi data'
    )

    args = parser.parse_args()

    print(sysconfig.get_paths()["purelib"])

    main(args)
