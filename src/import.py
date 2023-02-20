import re
import aiohttp, asyncio, requests
import datetime
import asyncpg
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from pandas import DataFrame
from typing import List
from sshtunnel import SSHTunnelForwarder

"""
ORDER OF IMPORTS:
1.  pokemon_info   - pokeapi [X]
2.  move_info      - pokeapi [ ]
3.  move_pool      - pokeapi [ ]
4.  egg_group*     - pokeapi [ ]
5.  metagame_info  - smogon  [ ]
6.  pokemon_stats  - smogon  [ ]
7.  nature_stats   - smogon  [ ]
8.  ability_stats  - smogon  [ ]
9.  teammate_stats - smogon  [ ]
10. item_stats     - smogon  [ ]
11. move_stats     - smogon  [ ]
*may end up being last form import due to multi-to-multi reasons.
"""

# Getting valid information from the connection file.
db_conn_file = open("./db_conn.key")
db_name = db_conn_file.readline().strip()
username = db_conn_file.readline().strip()
password = db_conn_file.readline().strip()


async def store_df(table_name: str, df: DataFrame):
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
        records = df.itertuples(index=False, name=None)
        await conn.copy_records_to_table(
            table_name,
            records=records,
            columns=list(df),
            schema_name=db_name,
            timeout=10,
        )
        conn.close()


async def pokemon_info_import():
    async def get_and_append(url: str, df: DataFrame):
        try:
            res = await session.get(url)
            json = await res.json()
            # dex number requires a seperate API call
            species_res = await session.get(json["species"]["url"])
            species_json = await species_res.json()
            # getting optional secondary type
            type_1, type_2 = json["types"][0]["type"]["name"], None
            if len(json["types"]) > 1:
                type_2 = json["types"][1]["type"]["name"]
            stats = json["stats"]
            # All the information for pokemon_info.
            row = [
                species_json["order"],
                json["name"],
                type_1,
                type_2,
                stats[0]["base_stat"],
                stats[1]["base_stat"],
                stats[2]["base_stat"],
                stats[3]["base_stat"],
                stats[4]["base_stat"],
                stats[5]["base_stat"],
                species_json["generation"]["name"],
            ]
            df.loc[len(df.index)] = row
        except Exception:
            print(url)

    async with aiohttp.ClientSession() as session:
        # Getting Pokémon in batches of 20 at a time.
        next = "https://pokeapi.co/api/v2/pokemon?limit=20&offset=0"
        offset = 0
        while next != None:
            df = DataFrame(
                columns=[
                    "dex_no",
                    "name",
                    "primary_type",
                    "secondary_type",
                    "base_hp",
                    "base_attack",
                    "base_defense",
                    "base_sp_attack",
                    "base_sp_defense",
                    "base_speed",
                    "generation",
                ]
            )
            res = await session.get(next)
            json = await res.json()
            print(f"Adding Pokemon {offset} - {len(json['results']) + offset - 1}")
            async with asyncio.TaskGroup() as tg:
                for val in json["results"]:
                    tg.create_task(get_and_append(val["url"], df))
            await store_df("pokemon_info", df)
            next = json["next"]
            offset += 20


async def main():
    pass
    # await pokemon_info_import()


asyncio.run(main())
