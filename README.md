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
6. `npm run create-parquet-files <START-DATE> [<END-DATE>]` - pulls and normalizes parameters and extras in remote log table and creates and saves parquet files locally.  If END-DATE is not provided it defaults to START-DATE (to push 1 day's worth)

## Uploading to S3

Once you have run all 6 scripts you can upload the parquet files to S3 using this command:

`cd output && aws s3 sync . s3://BUCKETNAME/processed_logs/` where BUCKETNAME is `log-ingester-qa` or `log-ingester-production`.  Before running make sure you have exported the correct aws profile to write to the buckets, eg:

`export AWS_PROFILE=qa`

(the aws cli is used instead of the scripts writing directly to S3 as I'm sure it is more performant)