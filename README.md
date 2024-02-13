# UDACITY-DWH-PROJECT   

This repository purpose is to develop an ETL and data warehouse solution.


Installation

```bash
pip install -r requirements.txt
```

Redshift Cluster Definition in dwh.cfg:
```bash
[CLUSTER]
HOST=[ENDPOINT WITHOUT PORT AND DATABASE NAME]
DB_NAME=[DATABASENAME]
DB_USER=[USERNAME]
DB_PASSWORD=[PASSWORD]
DB_PORT=[PORT]

[IAM_ROLE]
ARN='[ADD ARN]'
```
[S3]
LOG_DATA='[S3 path log data]'
LOG_JSONPATH='[S3 path log data format]'
SONG_DATA='[S3 path song data]