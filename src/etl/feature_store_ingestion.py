"""
Feature store ingestion script.
"""

import asyncio
import argparse
from datetime import datetime, timedelta

import asyncpg
from tqdm import tqdm


POSTGRES_URI = "postgresql://postgres:postgres@localhost:5401/olist"


def import_query(path) -> str:
    with open(path, "r", encoding="utf-8") as query_file:
        return query_file.read()


async def table_exists(conn, schema, table) -> bool:
    result = await conn.fetchrow(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
        schema,
        table,
    )
    return result[0]


async def create_from_query(conn, query, table_name) -> None:
    query = f"CREATE TABLE {table_name} AS {query}"
    await conn.execute(query)


async def delete_date(conn, table_name, date) -> None:
    await conn.execute(f"DELETE FROM {table_name} WHERE date_reference = '{date}'")


async def insert_from_query(conn, query, table_name) -> None:
    query = f"INSERT INTO {table_name} {query}"
    await conn.execute(query)


async def process_query(conn, query, dates, schema, table) -> None:
    table_name = f"{schema}.{table}"
    if not await table_exists(conn, schema, table):
        await create_from_query(conn, query.format(date=dates.pop(0)), table_name)
    for date in tqdm(dates):
        await delete_date(conn, table_name, date)
        await insert_from_query(conn, query.format(date=date), table_name)


def date_range(dt_start, dt_stop, period="monthly") -> list[str]:
    dates = []
    dt_start = datetime.strptime("2017-01-01", "%Y-%m-%d")
    dt_stop = datetime.strptime("2018-01-01", "%Y-%m-%d")
    while dt_start <= dt_stop:
        dates.append(dt_start.strftime("%Y-%m-%d"))
        dt_start += timedelta(days=1)
    if period == "daily":
        return dates
    if period == "monthly":
        return [dt for dt in dates if dt.endswith("01")]
    raise NotImplementedError()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--schema", type=str, default="analytics")
    parser.add_argument("--dt-start", type=str, default="2017-01-01")
    parser.add_argument("--dt-stop", type=str, default="2018-01-01")
    parser.add_argument("--period", type=str, default="monthly")
    args = parser.parse_args()

    table_name = f"fs_{args.table}"
    print(f"-> ingesting to table {table_name}")

    conn = await asyncpg.connect(POSTGRES_URI)
    query = import_query(f"src/etl/{args.table}.sql")

    dt_start = datetime.strptime(args.dt_start, "%Y-%m-%d")
    dt_stop = datetime.strptime(args.dt_stop, "%Y-%m-%d")
    dates = date_range(dt_start, dt_stop, period=args.period)

    await process_query(conn, query, dates, args.schema, table_name)
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
