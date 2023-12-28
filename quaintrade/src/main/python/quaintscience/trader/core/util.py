from dataclasses import field
from typing import Union
import functools
import datetime
import copy
import uuid

import sqlalchemy
import pandas as pd


def crossunder(df, col1, col2):
    if df.iloc[-2][col1] > df.iloc[-2][col2] and df.iloc[-1][col1] <= df.iloc[-1][col2]:
        return True
    return False

def crossover(df, col1, col2):
    if df.iloc[-2][col1] < df.iloc[-2][col2] and df.iloc[-1][col1] >= df.iloc[-1][col2]:
        return True
    return False

def get_datetime(dt: Union[str, datetime.datetime]):
    if isinstance(dt, str):
        try:
            dt = datetime.datetime.strptime(dt, "%Y%m%d %H:%M")
        except Exception:
            dt = datetime.datetime.strptime(dt, "%Y%m%d")
    return dt

def today_timestamp():
        return datetime.datetime.now().strftime("%Y%m%d")


def datestring_to_datetime(d):
    return datetime.datetime.strptime(d, "%Y%m%d")

def hash_dict(func):
    """Transform mutable dictionnary
    Into immutable
    Useful to be compatible with cache
    """
    class HDict(dict):
        def __hash__(self):
            return hash(frozenset(self.items()))

    class HList(list):
        def __hash__(self):
            return hash(frozenset(self))

    def freeze(arg):
        if isinstance(arg, dict):
            for k, v in arg.items():
                arg[k] = freeze(v)
            return HDict(arg)
        elif isinstance(arg, list):
            return HList([freeze(item) for item in arg])
        return arg


    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        args = tuple([freeze(arg) for arg in args])
        kwargs = {k: freeze(v) for k, v in kwargs.items()}
        return func(*args, **kwargs)
    return wrapped


def new_id():
    return str(uuid.uuid4()).replace("-", "")


def default_dataclass_field(obj):
    """Create a default field"""
    return field(default_factory=lambda: copy.copy(obj))

def current_datetime_field():
    """Create a default field"""
    return field(default_factory=lambda: datetime.datetime.now())

def new_id_field():
    """Create a default field"""
    return field(default_factory=lambda: new_id())


def resample_candle_data(data, interval):
    data = data.resample(interval).apply({'open': 'first',
                                            'high': 'max',
                                            'low': 'min',
                                            'close': 'last'})
    data.dropna(inplace=True)
    return data


def upsert_df(df: pd.DataFrame, table_name: str, engine: sqlalchemy.engine.Engine):
    """Implements the equivalent of pd.DataFrame.to_sql(..., if_exists='update')
    (which does not exist). Creates or updates the db records based on the
    dataframe records.
    Conflicts to determine update are based on the dataframes index.
    This will set primary keys on the table equal to the index names
    1. Create a temp table from the dataframe
    2. Insert/update from temp table into table_name
    Returns: True if successful
    """

    # If the table does not exist, we should just use to_sql to create it
    res = engine.execute(f"""SELECT name FROM sqlite_master WHERE type='table' and name=?;""", (table_name, ))
    exists = bool(res.fetchone())
    if not exists:
        df.to_sql(table_name, engine)
        return True

    # If it already exists...
    temp_table_name = f"temp_{uuid.uuid4().hex[:6]}"
    df.to_sql(temp_table_name, engine, index=True)

    index = list(df.index.names)
    index_sql_txt = ", ".join([f'"{i}"' for i in index])
    columns = list(df.columns)
    headers = index + columns
    headers_sql_txt = ", ".join(
        [f'"{i}"' for i in headers]
    )  # index1, index2, ..., column 1, col2, ...

    # col1 = exluded.col1, col2=excluded.col2
    update_column_stmt = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

    # For the ON CONFLICT clause, postgres requires that the columns have unique constraint
    query_pk = f"""
    ALTER TABLE "{table_name}" ADD CONSTRAINT {table_name}_unique_constraint_for_upsert UNIQUE ({index_sql_txt});
    """
    try:
        engine.execute(query_pk)
    except Exception as e:
        # relation "unique_constraint_for_upsert" already exists
        if not 'unique_constraint_for_upsert" already exists' in e.args[0]:
            raise e

    # Compose and execute upsert query
    query_upsert = f"""
    INSERT INTO "{table_name}" ({headers_sql_txt}) 
    SELECT {headers_sql_txt} FROM "{temp_table_name}"
    ON CONFLICT ({index_sql_txt}) DO UPDATE 
    SET {update_column_stmt};
    """
    engine.execute(query_upsert)
    engine.execute(f'DROP TABLE "{temp_table_name}"')

    return True
