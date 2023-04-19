"""
Insert CSV data into the PostgreSQL database.
"""

import asyncio

import numpy as np
import pandas as pd
import asyncpg


POSTGRES_URI = "postgresql://postgres:postgres@localhost:5401/olist"


async def insert_fn(pool, query, batch):
    await pool.executemany(query, batch)


async def insert_data(pool: asyncpg.Pool, data_table: pd.DataFrame, query: str):
    batch = []
    tasks = set()
    for _, row in data_table.iterrows():
        if row is None:
            continue
        batch.append(row)
        if len(batch) >= 1024:
            task = asyncio.create_task(insert_fn(pool, query, batch))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            batch = []
        if len(tasks) > 10:
            await asyncio.gather(*tasks)
    if batch:
        tasks.add(asyncio.create_task(insert_fn(pool, query, batch)))
    await asyncio.gather(*tasks)


async def main():
    pool = await asyncpg.create_pool(POSTGRES_URI)
    csv_tables = [
        ("./data/olist_customers_dataset.csv", "customers"),
        ("./data/olist_orders_dataset.csv", "orders"),
        ("./data/olist_products_dataset.csv", "products"),
        ("./data/olist_customers_dataset.csv", "customers"),
        ("./data/olist_geolocation_dataset.csv", "geolocation"),
        ("./data/olist_order_items_dataset.csv", "order_items"),
        ("./data/olist_order_reviews_dataset.csv", "order_reviews"),
        ("./data/olist_sellers_dataset.csv", "sellers"),
    ]

    for csv_file, table in csv_tables:
        print(f"inserting {table}")
        df = pd.read_csv(csv_file)
        datelike_cols = [
            col for col in df if "date" in col or "timestamp" in col or "approved_at" in col
        ]
        df[datelike_cols] = df[datelike_cols].apply(pd.to_datetime)
        df = df.replace({np.NaN: None})
        query_values = "(" + ",".join([f"${i + 1}" for i in range(df.shape[1])]) + ")"
        query = f"INSERT INTO {table} VALUES " + query_values + " ON CONFLICT DO NOTHING"
        await insert_data(pool, df, query)


if __name__ == "__main__":
    asyncio.run(main())
