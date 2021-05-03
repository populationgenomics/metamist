# Sample Metadata

An API to manage sample + other related metadata.

## First time setup

Add the postgres jar into the db folder:

```
pushd db/
curl https://jdbc.postgresql.org/download/postgresql-42.2.20.jar
```

## Model updates

### Adding an enum value

If you add a new value for an enum, ensure you add a schema migration, eg:

```xml
<sql>ALTER TYPE sample_type ADD VALUE hair</sql>
```

