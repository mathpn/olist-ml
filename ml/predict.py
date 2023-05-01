from datetime import datetime

import mlflow
import mlflow.sklearn
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

POSTGRES_URI = "postgresql://postgres:postgres@localhost:5401/olist"
pd.set_option("display.max_rows", None)


def table_exists(cursor, schema, table) -> bool:
    cursor.execute(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
        (schema, table),
    )
    return cursor.fetchone()[0]


def create_from_query(cursor, query, table_name) -> None:
    query = f"CREATE TABLE {table_name} AS {query}"
    cursor.execute(query)


def delete_date(cursor, table_name, date) -> None:
    cursor.execute(
        f"DELETE FROM {table_name} WHERE date_score = '{date}' AND desc_model = 'seller churn'"
    )


def insert_from_query(cursor, query, table_name) -> None:
    query = f"INSERT INTO {table_name} {query}"
    cursor.execute(query)


def main():
    model = mlflow.sklearn.load_model("models:/olist_churn_model/10")
    conn = create_engine(POSTGRES_URI)
    pg_conn = psycopg2.connect(POSTGRES_URI)

    with open("etl/fs_join.sql", "r") as fs_join_file:
        fs_join_query = fs_join_file.read()

    print("-> updating fs_join table")
    pg_conn.execute(fs_join_query)

    df = pd.read_sql("SELECT * FROM analytics.abt_olist_churn", conn)
    print(f"-> predicting {len(df)} samples")
    predict = model.predict_proba(df[model.feature_names_in_])[:, 1]

    df_extract = df[["seller_id"]].copy()
    df_extract["score"] = predict
    df_extract["desc_model"] = "seller churn"
    dt_now = datetime.now().strftime("%Y-%m-%d")
    df_extract["date_score"] = df["date_reference"][0]
    df_extract["date_ingestion"] = dt_now
    df_extract.head()

    cursor = pg_conn.cursor()
    if not table_exists(cursor, "analytics", "olist_models"):
        print("-> creating prediction table")
        df_extract.to_sql("olist_models", conn, schema="analytics", if_exists="append")
    else:
        print("-> updating prediction table")
        delete_date(cursor, "analytics.olist_models", dt_now)
        df_extract.to_sql("olist_models", conn, schema="analytics", if_exists="append")

    pg_conn.commit()


if __name__ == "__main__":
    main()
