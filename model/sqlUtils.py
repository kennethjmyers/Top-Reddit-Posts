import pandas as pd
import sqlalchemy
import uuid
import os
from sqlalchemy import text
from sqlalchemy import create_engine


def makeEngine(cfg):
  """
  Returns a sql engine using passed config

  :param cfg: dictionary containing username, password, host, port, and database
  :return: engine
  """
  sqlcfg = cfg['Postgres']
  return create_engine(
      f"postgresql+pg8000://{sqlcfg['USERNAME']}:{sqlcfg['PASSWORD']}@{sqlcfg['HOST']}:{sqlcfg['PORT']}/{sqlcfg['DATABASE']}"
  )


def upsert_df(df: pd.DataFrame, table_name: str, engine: sqlalchemy.engine.Engine):
    """ From: https://stackoverflow.com/a/69617559/5034651
    Note: before using this, the df you pass should have an index set by using df.set_index([col1, col2, ...])
    This won't work if it doesn't have any named indexes.

    Implements the equivalent of pd.DataFrame.to_sql(..., if_exists='update')
    (which does not exist). Creates or updates the db records based on the
    dataframe records.
    Conflicts to determine update are based on the dataframes index.
    This will set unique keys constraint on the table equal to the index names
    1. Create a temp table from the dataframe
    2. Insert/update from temp table into table_name

    :returns: True if successful
    """

    # If the table does not exist, we should just use to_sql to create it
    if not engine.execute(
        text(f"""SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'public'
            AND    table_name   = '{table_name}');
            """)
    ).first()[0]:
        df.to_sql(table_name, engine)
        return True

    # If it already exists...
    temp_table_name = f"temp_{uuid.uuid4().hex[:6]}"
    df.to_sql(temp_table_name, engine, index=True)

    try:  # if something errors out from here onward, we still want to drop the temp table before raising error
      index = list(df.index.names)
      index_sql_txt = ", ".join([f'"{i}"' for i in index])
      columns = list(df.columns)
      headers = index + columns
      headers_sql_txt = ", ".join(
          [f'"{i}"' for i in headers]
      )  # index1, index2, ..., column 1, col2, ...  Indexes here are the index names, not row numbers

      # col1 = exluded.col1, col2=excluded.col2
      update_column_stmt = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

      # For the ON CONFLICT clause, postgres requires that the columns have unique constraint
      constraint_name = f"unique_constraint_for_{table_name}_upsert"
      query_pk = f"""
      ALTER TABLE "{table_name}" DROP CONSTRAINT IF EXISTS {constraint_name};
      ALTER TABLE "{table_name}" ADD CONSTRAINT {constraint_name} UNIQUE ({index_sql_txt});
      """  # it's saying the index names must be unique
      engine.execute(query_pk)

      # Compose and execute upsert query
      query_upsert = f"""
      INSERT INTO "{table_name}" ({headers_sql_txt}) 
      SELECT {headers_sql_txt} FROM "{temp_table_name}"
      ON CONFLICT ({index_sql_txt}) DO UPDATE 
      SET {update_column_stmt};
      """
      engine.execute(query_upsert)
      engine.execute(f"DROP TABLE {temp_table_name}")
    except:
      engine.execute(f"DROP TABLE {temp_table_name}")
      raise

    return True
