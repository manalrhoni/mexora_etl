# الفيشي: mexora_etl/load/loader.py

import sqlalchemy
import pandas as pd
from sqlalchemy import text

def charger_dimension(df: pd.DataFrame, table_name: str, engine, if_exists='replace'):
    # استعملنا text() باش بايثون يفهم بلي هادا كود SQL، و engine.begin() باش يـ valider
    with engine.begin() as con:
        con.execute(text(f"DROP TABLE IF EXISTS dwh_mexora.{table_name} CASCADE;"))
    
    # دابا غيلوح الداتا نقية
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
    # Pour simplifier et éviter les erreurs SQLAlchemy complexes avant la création des tables
    # On utilise la méthode 'append' de pandas
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