import sqlalchemy
import pandas as pd
from sqlalchemy import text

def charger_dimension(df: pd.DataFrame, table_name: str, engine, if_exists='replace'):
    with engine.begin() as con:
        con.execute(text(f"DROP TABLE IF EXISTS dwh_mexora.{table_name} CASCADE;"))
    
    df.to_sql(
        name=table_name,
        con=engine,
        schema='dwh_mexora',
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    print(f"[LOAD] {table_name} : {len(df)} lignes chargées")

def charger_faits(df: pd.DataFrame, engine):
    """
    Charge la table de faits.
    """
    df.to_sql(
        name='fait_ventes',
        con=engine,
        schema='dwh_mexora',
        if_exists='append',
        index=False,
        method='multi',
        chunksize=5000
    )
    print(f"[LOAD] fait_ventes : {len(df)} lignes chargées")