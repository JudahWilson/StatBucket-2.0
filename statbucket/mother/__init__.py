from sqlalchemy import create_engine, MetaData, Table, select, insert
import os
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(os.environ["PROD_DB_URL"])
"""Production database"""
engine_staged = create_engine(os.environ["STAGING_DB_URL"])
"""Intermediate storage for review"""


# TODO need to test or deletesss
def transfer_table(
    src_engine,
    dst_engine,
    table_name,
    chunk_size=1000,
):
    src_meta = MetaData()
    dst_meta = MetaData()

    # Reflect table from both DBs
    src_table = Table(table_name, src_meta, autoload_with=src_engine)
    dst_table = Table(table_name, dst_meta, autoload_with=dst_engine)

    # Columns to copy (exclude PKs)
    copy_columns = [c.name for c in src_table.columns if not c.primary_key]

    if not copy_columns:
        raise ValueError(f"No non-PK columns found for table {table_name}")

    select_stmt = select(*[src_table.c[c] for c in copy_columns])
    insert_stmt = insert(dst_table).values([{c: None for c in copy_columns}])

    with src_engine.connect() as src_conn, dst_engine.begin() as dst_conn:
        result = src_conn.execute(select_stmt)

        batch = []
        for row in result.mappings():
            batch.append({c: row[c] for c in copy_columns})

            if len(batch) >= chunk_size:
                dst_conn.execute(insert(dst_table), batch)
                batch.clear()

        if batch:
            dst_conn.execute(insert(dst_table), batch)
