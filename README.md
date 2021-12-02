# s3-log-importer
Scripts to import Postgres log data into S3 as parquet files

This repo contains a set of scripts to import normalized log data from the existing Postgres log
table into S3 as parquet files using the YYYY/MM/DD/HH path prefix.

The scripts maintain a small local database with metadata of rows in the remote log table.

## Configuration

Copy the .env.sample file to .env and fill in the values.

## Scripts

1. `npm run db-create` - MUST BE RUN FIRST. Creates local database, tables and indices.
2. `npm run get-ids` - returns the min and max ids from the local and remote database.
3. `npm run pull-logs <START-ID> <END-ID>` - pulls metadata from the remote log table into the local database.
4. `npm run get-times` - returns the min and max times from the local and remote database.
5. `npm run normalize-timestamps` - normalizes all the timestamps in the local database.
6. `npm run push-logs <START-DATE> <END-DATE>` - pulls and normalizes parameters and extras in remote log table and creates and saves parquet file on S3.