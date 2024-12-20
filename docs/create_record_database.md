# Creating the record database

## Contents

1. [Creating a persistent database](#creating-a-persistent-database)
1. [Inserting country information](#inserting-country-information)
1. [Filtering and inserting record information](#filtering-and-inserting-record-information)

---

### Creating a persistent database

```python
from lib.sql_interface import DataInterface

connection = DataInterface.open_connection('example.db')
DataInterface.create_blank_database(connection)
```

---

### Inserting country information

As records make an FK relationship to the Country table, insert these data ahead of time.

```bash
wget https://www.gdeltproject.org/data/lookups/CAMEO.country.txt
```

```python
import polars as pl
from lib.sql_country import Country
from lib.sql_interface import DataInterface

connection = DataInterface.open_connection('example.db')

df = pl.read_csv('data/CAMEO.country.txt', separator='\t')

for (code, name) in df.iter_rows():
    Country.create_record(connection, code, name)

# Confirm that the insertion was successful.
Country.select_all(connection)
```

|Code|Name|
|:---:|:---|
|WSB|West Bank|
|BAG|Baghdad|
|GZS|Gaza Strip|
|AFR|Africa|
|ASA|Asia|
|...|...|
|WLF|Wallis and Futuna Islands|
|ESH|Western Sahara|
|YEM|Yemen|
|ZMB|Zambia|
|ZWE|Zimbabwe|

---

### Obtaining record information

```bash
wget http://data.gdeltproject.org/events/GDELT.MASTERREDUCEDV2.1979-2013.zip
unzip GDELT.MASTERREDUCEDV2.1979-2013.zip
```

Structure:

|Columns|Description|
|:---:|:---|
|Date|YYYYMMDD Date stamp for the record.|
|Source, Target|Country code for the source/target of the event.|
|CAMEOCode|CAMEO code recording the interaction type. Documentation is [here](http://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf).|
|NumEvents, NumArts|Number of events recorded, number of articles referencing the event.|
|QuadClass|Primary taxonomic classification for the CAMEO event, verbal/material, cooperation/conflict.|
|Goldstein|Rating of the potential for impact on the country.|
|SourceGeoType, SourceGeoLat, SourceGeoLong|Specific location of the source. Type refers to granaularity as below.|
|TargetGeoType, TargetGeoLat, TargetGeoLong|Specific location of the target. Type refers to granaularity as below.|
|ActionGeoType, ActionGeoLat, ActionGeoLong|Specific location of the event. Type refers to granaularity as below.|

>Type values:
>
>1. Country
>2. US State
>3. US City
>4. World city
>5. World state

---

### Filtering and inserting GeoTag information

As part of normalising the database, I've pulled the source/target/action columns into a separate table where they can be uniquely stored. Populate this, as for the Country records above.

```python
import polars as pl
from lib.sql_country import Country
from lib.sql_geotag import GeoTag
from lib.sql_interface import DataInterface

connection = DataInterface.open_connection('example.db')

# Filter to only retain record tags from countries in the database
accepted_codes = Country.select_all(connection).get_column('Code')

df = (
    pl
    .scan_csv('data/GDELT.MASTERREDUCEDV2.TXT', separator='\t')
    .filter(
        pl.col('Source').is_in(accepted_codes),
        pl.col('Target').is_in(accepted_codes),
    )
    .select('SourceGeoType', 'SourceGeoLat', 'SourceGeoLong', 'TargetGeoType', 'TargetGeoLat', 'TargetGeoLong', 'ActionGeoType', 'ActionGeoLat', 'ActionGeoLong')
    .collect()
)

# A bit of trickery to turn this into a single, unique set of records
narrow_df = (
    pl.concat([
            df.select('SourceGeoType', 'SourceGeoLat', 'SourceGeoLong').rename(lambda x: x[-4:]),
            df.select('TargetGeoType', 'TargetGeoLat', 'TargetGeoLong').rename(lambda x: x[-4:]),
            df.select('ActionGeoType', 'ActionGeoLat', 'ActionGeoLong').rename(lambda x: x[-4:]),
        ],
        how='vertical'
    )
    .drop_nulls()
    .unique()
    .rename({'Type': 'geo_type', 'oLat': 'geo_lat', 'Long': 'geo_long'})
)
# This drops from 16,157,745 records to just 207,428

GeoTag.create_mass_records(connection, narrow_df.to_dicts())
GeoTag.select_all(connection)
```

┌────────┬─────────┬─────────┬──────────┐
│ Geo_ID ┆ GeoType ┆ GeoLat  ┆ GeoLong  │
│ ---    ┆ ---     ┆ ---     ┆ ---      │
│ i64    ┆ i64     ┆ f64     ┆ f64      │
╞════════╪═════════╪═════════╪══════════╡
│ 1      ┆ 4       ┆ 14.3739 ┆ 100.485  │
│ 2      ┆ 4       ┆ 42.5766 ┆ 1.66871  │
│ 3      ┆ 4       ┆ 67.4    ┆ 58.3667  │
│ 4      ┆ 3       ┆ 43.2551 ┆ -77.6169 │
│ 5      ┆ 4       ┆ 33.2758 ┆ 35.1942  │
│ …      ┆ …       ┆ …       ┆ …        │
│ 207424 ┆ 4       ┆ 53.36   ┆ 6.426    │
│ 207425 ┆ 4       ┆ 53.6833 ┆ 11.4833  │
│ 207426 ┆ 3       ┆ 31.3224 ┆ -92.4346 │
│ 207427 ┆ 4       ┆ 8.27306 ┆ -10.31   │
│ 207428 ┆ 4       ┆ -7.4906 ┆ 112.582  │

---

### Filtering and inserting record information

Finally, the residual information for each record in the `GDELT.MASTERREDUCEDV2.TXT` file needs to be inserted. Due to the size of the data, this is done as a bulk insert statement, which means the `Country` and `GeoTag` information is passed directly by their foreign keys, not as ORM objects.

```python
import polars as pl
from lib.sql_country import Country
from lib.sql_gdelt_record import GdeltRecord
from lib.sql_geotag import GeoTag
from lib.sql_interface import DataInterface

connection = DataInterface.open_connection('example.db')
accepted_codes = Country.select_all(connection).get_column('Code')

# Parse record table
df = (
    pl
    .scan_csv(
        'data/GDELT.MASTERREDUCEDV2.TXT',
        separator='\t',
        schema_overrides={'Date': pl.Utf8},
        null_values={'CAMEOCode': '---'},
    )
    .drop_nulls(subset=['CAMEOCode'])
    .filter(
        pl.col('Source').is_in(accepted_codes),
        pl.col('Target').is_in(accepted_codes),
    )
    .with_columns(
        year=pl.col('Date').map_elements(lambda x: x[0:4], return_dtype=pl.Utf8),
        month=pl.col('Date').map_elements(lambda x: x[4:6], return_dtype=pl.Utf8),
        day=pl.col('Date').map_elements(lambda x: x[6:], return_dtype=pl.Utf8),
    )
    .with_columns(
        Date=pl.date(pl.col('year'), pl.col('month'), pl.col('day'))
    )
    .drop(['year', 'month', 'day'])
    .rename({
        'Date': 'date',
        'Source': 'source_id',
        'Target': 'target_id',
        'CAMEOCode': 'cameo_code',
        'NumEvents': 'num_events',
        'NumArts': 'num_arts',
        'QuadClass': 'quad_class',
        'Goldstein': 'goldstein',
    })
    .collect()
)

# Map as dictionaries for bulk insertion. This prevents 16 million queries to match the IDs
def to_key(x, y, z):
    return f"{x}|{y}|{z}"

geotags = {
    to_key(*row[1:]): row[0]
    for row in pl.read_database(query="SELECT * FROM GEO_TAG", connection=connection).iter_rows()
}

df = (
    df
    .with_columns(
        source_record_id=pl.struct(['SourceGeoType', 'SourceGeoLat', 'SourceGeoLong']).map_elements(lambda x: to_key(*x.values()), return_dtype=pl.Utf8),
        target_record_id=pl.struct(['TargetGeoType', 'TargetGeoLat', 'TargetGeoLong']).map_elements(lambda x: to_key(*x.values()), return_dtype=pl.Utf8),
        action_record_id=pl.struct(['ActionGeoType', 'ActionGeoLat', 'ActionGeoLong']).map_elements(lambda x: to_key(*x.values()), return_dtype=pl.Utf8),
    )
    .with_columns(
        source_record_id=pl.col('source_record_id').map_elements(lambda x: geotags.get(x, None), return_dtype=pl.Int64),
        target_record_id=pl.col('target_record_id').map_elements(lambda x: geotags.get(x, None), return_dtype=pl.Int64),
        action_record_id=pl.col('action_record_id').map_elements(lambda x: geotags.get(x, None), return_dtype=pl.Int64),
    )
    .select(
        'date', 'source_id', 'target_id', 'cameo_code', 'num_events', 'num_arts', 'quad_class', 'goldstein',
        'source_record_id', 'target_record_id', 'action_record_id')
)

# Insert record information
GdeltRecord.create_mass_records(connection, df.to_dicts())
```

Prior to splitting out the `GeoTag` data, the database size was about 1.3 GB. With GeoTag dereplication it is about 800 MB.

---
