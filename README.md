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
import db_connect as db
```

At the top of the file you wish to connect to the
database from. This function returns a connection object you can perform operations on. For example:

```py
with db.tunnel() as server: 
    conn = await db_connect(server)
    query = conn.fetch("""SELECT * FROM table;""")
    conn.close()
```

Is a valid way of accessing the database. **CLOSE THE CONNECTION AFTER OPENING IT, ALWAYS**.
