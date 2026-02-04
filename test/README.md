# Running tests locally

Running tests requires mariadb-java client, docker (for mariadb), and all the regular dev dependencies.

Install mariadb-java client with:

```shell
cd db
wget https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.0.3/mariadb-java-client-3.0.3.jar
```

If you have these installed, you can run the tests on the terminal with:

```shell
uv run -m unittest discover -s test/
```

Otherwise, in VSCode:

- Make sure your VSCode knows your python virtual environment version (`which python`)
- Then from the using the command palette (cmd+shift+P), you can "Python: Configure Tests" with:
    - `unittest`
    - `test/` folder
    - `test_*.py` format
- If that doesn't work, try using `pytest`. Again use the command palette and "Python: Configure Tests"
    - `pytest`
    - `test/` folder

This should display a full list of Python tests which you run all, or debug individual tests.
