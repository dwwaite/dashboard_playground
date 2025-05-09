# Table query optimisation

Given the size of this data set, getting query results back to the UI as quickly as possible is a priority. The standard query approach with SQLAlchemy returns row-wise tuples, and while it's quite quick these need to be converted into a polars data frame for use in the interface. This notebook baselines different query implementations for to assess their speed.

All trials query the GDelt records table, matching all records against a single country match.

## Contents

1. [SQLAlchemy with row-wise conversion](#sqlalchemy-with-row-wise-conversion)
1. [SQLAlchemy with column-wise conversion](#sqlalchemy-with-column-wise-conversion)
1. [SQLAlchemy with preallocated column-wise conversion](#sqlalchemy-with-preallocated-column-wise-conversion)
1. [Polars SQL interface](#polars-sql-interface)
1. [Conclusions](#conclusions)

---

## SQLAlchemy with row-wise conversion

```python
import timeit
import polars as pl
from sqlalchemy import select
from sqlalchemy.orm import Session
from lib.sql_gdelt_record import Country, GdeltRecord
from lib.sql_interface import DataInterface

def gdelt_to_dict(gd):
    return {
        'date': gd.date, 'source_id': gd.source_id, 'target_id': gd.target_id, 'cameo_code': gd.cameo_code,
        'num_events': gd.num_events, 'num_arts': gd.num_arts, 'quad_class': gd.quad_class, 'goldstein': gd.goldstein,
        'source_record_id': gd.source_record_id, 'target_record_id': gd.target_record_id, 'action_record_id': gd.action_record_id,
    }

def rowwise_query(engine, source_country):
    with Session(engine) as session:
        results = session.scalars(
            select(GdeltRecord)
            .where(GdeltRecord.source_id == source_country)
        ).all()
    return pl.DataFrame([gdelt_to_dict(result) for result in results])

connection = DataInterface.open_connection('data/example.db')

timeit.timeit(
    "_ = rowwise_query(connection, 'AUS')",
    setup='from __main__ import connection, rowwise_query, gdelt_to_dict',
    number=10
)
```

---

## SQLAlchemy with column-wise conversion

```python
import timeit
import polars as pl
from sqlalchemy import select
from sqlalchemy.orm import Session
from lib.sql_gdelt_record import Country, GdeltRecord
from lib.sql_interface import DataInterface

def sweep_series(gd_list, feature):
    return pl.Series(feature, [getattr(gd, feature) for gd in gd_list])

def colwise_query(engine, source_country):
    features = [
        'date', 'source_id', 'target_id', 'cameo_code',
        'num_events', 'num_arts', 'quad_class', 'goldstein',
        'source_record_id', 'target_record_id', 'action_record_id'
    ]
    with Session(engine) as session:
        results = session.scalars(
            select(GdeltRecord)
            .where(GdeltRecord.source_id == source_country)
        ).all()
    return pl.DataFrame([sweep_series(results, feature) for feature in features])

connection = DataInterface.open_connection('data/example.db')

timeit.timeit(
    "_ = colwise_query(connection, 'AUS')",
    setup='from __main__ import connection, colwise_query, sweep_series',
    number=10
)
```

---

## SQLAlchemy with preallocated column-wise conversion

Similar concept as above, but this time preallocating the lists to allow for a single iteration of the results.

```python
import timeit
import polars as pl
from sqlalchemy import select
from sqlalchemy.orm import Session
from lib.sql_gdelt_record import Country, GdeltRecord
from lib.sql_interface import DataInterface

def results_to_df(gd_list):
    n_gds = len(gd_list)
    features = [
        'date', 'source_id', 'target_id', 'cameo_code',
        'num_events', 'num_arts', 'quad_class', 'goldstein',
        'source_record_id', 'target_record_id', 'action_record_id'
    ]
    feature_map = {feature: [None] * n_gds for feature in features}
    for i, gd in enumerate(gd_list):
        for feature in feature_map.keys():
            feature_map[feature][i] = getattr(gd, feature)
    return pl.DataFrame([pl.Series(key, value) for key, value in feature_map.items()])

def opt_colwise_query(engine, source_country):
    with Session(engine) as session:
        results = session.scalars(
            select(GdeltRecord)
            .where(GdeltRecord.source_id == source_country)
        ).all()
    return results_to_df(results)

connection = DataInterface.open_connection('data/example.db')

timeit.timeit(
    "_ = opt_colwise_query(connection, 'AUS')",
    setup='from __main__ import connection, opt_colwise_query, results_to_df',
    number=10
)
```

---

## Polars SQL interface

```python
import timeit
import polars as pl
from lib.sql_interface import DataInterface

def polars_sql(engine, source_country):
    return(
        pl
        .read_database(
            query="""
                SELECT date, source_id, target_id, cameo_code, num_events, num_arts, quad_class, goldstein, source_record_id, target_record_id, action_record_id
                FROM GDELT_RECORD
                WHERE source_id == :value
            """,
            connection=engine,
            execute_options={'parameters': {'value': source_country}}
        )
    )

connection = DataInterface.open_connection('data/example.db')

timeit.timeit(
    "_ = polars_sql(connection, 'AUS')",
    setup='from __main__ import connection, polars_sql',
    number=10
)
```

---

## Conclusions

|Method|Run time|
|:---|:---:|
|SQLAlchemy (row-wise)|51.6|
|SQLAlchemy (column-wise)|48.6|
|SQLAlchemy (preallocated)|46.2|
|Polars SQL|27.0|

There's a clear difference in times to execute - using the native polars SQL reader is about half the run time of other methods.

This does make sense, as the polars engine can make the cast to a dataframe directly, whereas the other methods need to create the python tuple objects then iterate to pull pity and format the content.

Straight Polars will be the way forward for adding query capability.
