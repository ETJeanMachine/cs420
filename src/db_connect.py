import asyncpg
from asyncpg.connection import Connection

db_conn_file = open("./db_conn.key")
db_name = db_conn_file.readline().strip()
schema = db_conn_file.readline().strip()
username = db_conn_file.readline().strip()
password = db_conn_file.readline().strip()


async def db_connect() -> Connection:
    with SSHTunnelForwarder(
        ("starbug.cs.rit.edu", 22),
        ssh_username=username,
        ssh_password=password,
        remote_bind_address=("localhost", 5432),
    ) as server:
        server.start()
        params = {
            "database": db_name,
            "user": username,
            "password": password,
            "host": "localhost",
            "port": server.local_bind_port,
        }
        conn: Connection = await asyncpg.connect(**params)
        await conn.execute(f"""set search_path = "{schema}";""")
        return conn
