# CSCI 420 Project Github

## Collaborators

- Peter Carbone
- Eric Hamilton
- Nicholas Lewandowski
- Drake Zirkle

## Links to data sources

- [Smogon](https://www.smogon.com/stats/)
- [PokeApi](https://pokeapi.co/)

## HOW-TO

- Ensure you are on python version 3.11 or higher.
  - Check via `python --version`.
- Run `python -m pip install requirements.txt`.
- Insert the database, schema, username, and password as three lines in a new file called `db_conn.key`, in the root
  directory (not in `/src`).

  - Example `db_conn.key`:

  ```key
  sample_db
  sample_schema
  user123
  pass123
  ```

## Connecting in Code

To connect within the program, put:

```py
import asyncio
import utils.db_connect as db
```

At the top of the file you wish to connect to the database from. The `utils.db_connect` file contains helper functions to make connecting to the database easier. `asyncio` is necessary for running asynchronous calls to the database.

```py
async main():
    with db.tunnel() as server: 
        conn = await db.connect(server)
        records = await conn.fetch("""SELECT * FROM table;""")
        # How to convert to a pandas dataframe:
        # df = pd.Dataframe.from_records(records)
        conn.close()

asyncio.run(main())
```


Is a valid way of accessing the database. **CLOSE THE CONNECTION AFTER OPENING IT, ALWAYS**.
