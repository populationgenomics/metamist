# Sample Metadata

An API to manage sample + other related metadata.

## First time setup

Add the postgres jar into the db folder:

```bash
pushd db/
curl https://jdbc.postgresql.org/download/postgresql-42.2.20.jar
wget https://cdn.mysql.com//Downloads/Connector-J/mysql-connector-java-8.0.24.zip

# ensure you have the database created
psql -U postgres -c "CREATE DATABASE sm_dev;"

# now you can run liquibase
liquibase --changeLogFile 2021-05-03_initial.xml update
```

## Debugging

Due to the way python imports work, you're unable to run files directly. To run files, you must run them as a module, for example, to run the `db/python/layers/sample.py` file directly (in case you put an `if __name__ == "__main__"` block in there), you could use the following:

```bash
# convert '/' to '.' and drop the '.py'
python -m db.python.layers.sample
```

### VSCode

VSCode allows you to debug python modules, using the previous line as inspiration, we can consider the following `launch.json`:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: SampleLayer",
            "type": "python",
            "request": "launch",
            "module": "db.python.layers.sample"
        }
    ]
}
```

## Model updates

### Adding an enum value

If you add a new value for an enum, ensure you add a schema migration, eg:

```xml
<sql>ALTER TYPE sample_type ADD VALUE hair</sql>
```
