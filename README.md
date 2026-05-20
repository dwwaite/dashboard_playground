# Getting started

While I'm finding my feet here, will use the older version of GDELT with reduced granularity (daily results, not 15 minute snapshops) and is significantly smaller (1 GB vs 2.5 TB). The table columns are a subset of the full version described [here](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf).

## Contents

1. [Database creation and downsampling for testing](./docs/0.create_record_database.md)
1. [Query optimisation - SQLalchemy vs polars](./docs/1.table_query_optimisation.md)

---

## Execute

```bash
streamlit run app.py
```

---

## Known issues

1. Something is wrong with the dynamic grouping at month level.
   1. Grouping/display is correct for day and year, but duplicates entries when month is specified.

---
