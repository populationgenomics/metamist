# Running tests locally

Running tests requires docker (for mariadb), and all the regular dev dependencies.

If you have these installed, you can run the tests on the terminal with:

```shell
python -m unittest discover -s test/
```

Otherwise, in VSCode:

- Make sure your VSCode knows your python virtual environment version (`which python`)
- Then from the "Testing" tab, you can "Configure Python Tests" with:
    - `unittest`
    - `test/` folder
    - `test_*.py` format

This should display a full list of Python tests which you run all, or debug individual tests.
