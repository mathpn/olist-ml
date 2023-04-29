# %%
import mlflow
import mlflow.sklearn
import pandas as pd
from sqlalchemy import create_engine

POSTGRES_URI = "postgresql://postgres:postgres@localhost:5401/olist"
pd.set_option('display.max_rows', None)
# %%
model = mlflow.sklearn.load_model("models:/olist_churn_model/10")
conn = create_engine(POSTGRES_URI)
# %%
df = pd.read_sql("SELECT * FROM analytics.abt_olist_churn", conn)
predict = model.predict_proba(df[model.feature_names_in_])[:, 1]
# %%
predict