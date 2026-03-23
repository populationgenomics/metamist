# Setup Postgres DB

## Build and Run

> docker compose up -d

## Build

Build the database
> docker compose -f docker-compose.yaml build

## Run

Run detached (-d)
> docker compose run -d postgres


## Regenerate schema.sql file


```bash
docker compose exec postgres dbmate --schema-file /dev/stdout dump > schema.sql
```
