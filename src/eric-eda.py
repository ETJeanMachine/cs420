from datetime import date
import aiohttp, asyncio, asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pandas import DataFrame
from sshtunnel import SSHTunnelForwarder

db_conn_file = open("./db_conn.key")
db_name = db_conn_file.readline().strip()
username = db_conn_file.readline().strip()
password = db_conn_file.readline().strip()


async def main():
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
        conn = await asyncpg.connect(**params)
        await conn.execute("""set search_path = "p42002_03";""")
        query = """
SELECT p2.name,
	   m2.month,
	   sum(ps2.raw_count)::float / sum(m2.total_battles) as percent
from pokemon_info p2,
	 metagame_info m2,
	 pokemon_stats ps2
		 inner join (SELECT ps.pokemon_info_id
					 from pokemon_stats ps,
						  pokemon_info p,
						  metagame_info m
					 where ps.pokemon_info_id = p.pokemon_info_id
					   and m.metagame_id = ps.metagame_id
					   and m.cutoff = 0
					 group by ps.pokemon_info_id
					 order by sum(ps.raw_count) desc
					 limit 5) t on t.pokemon_info_id = ps2.pokemon_info_id
WHERE m2.metagame_id = ps2.metagame_id
  AND ps2.pokemon_info_id = p2.pokemon_info_id
  AND m2.cutoff = 0
group by month, name;"""
        q1_records = await conn.fetch(query)
        conn.close()
    df = DataFrame.from_records(q1_records)
    pokemon = df[0].unique()
    for p in pokemon:
        vals = df[df[0] == p]
        plt.plot(vals[1], vals[2], label=p)
    plt.legend()
    plt.ylabel("Usage")
    plt.xlabel("Time")
    plt.show()
    max_land = df[df[2] == df[2].max()]
    print(max_land)


asyncio.run(main())
