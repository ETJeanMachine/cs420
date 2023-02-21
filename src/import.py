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
2.  move_info      - pokeapi [X]
3.  move_pool      - pokeapi [X]
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


async def append_df(table: str, df: DataFrame):
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
            table,
            records=records,
            columns=list(df),
            schema_name=db_name,
            timeout=10,
        )
        conn.close()


async def get_pokemon_info(url: str, df: DataFrame):
    try:
        async with aiohttp.ClientSession() as session:
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


async def get_move_info(url: str, df: DataFrame):
    async with aiohttp.ClientSession() as session:
        res = await session.get(url)
        json = await res.json()
        if json["type"]["name"] == "shadow":
            return
        row = [
            json["name"],
            json["type"]["name"],
            json["damage_class"]["name"],
            json["power"],
            json["accuracy"],
            json["pp"],
            json["priority"],
        ]
        df.loc[len(df.index)] = row


async def pre_process_movepool():
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
        pkmn_res = await conn.fetch(
            f"SELECT name, pokemon_info_id FROM {db_name}.pokemon_info"
        )
        move_res = await conn.fetch(
            f"SELECT name, move_id FROM {db_name}.move_info"
        )
        pokemon = np.array(pkmn_res)
        moves = np.array(move_res)
    async def get_movepool(url: str, df: DataFrame):
        async with aiohttp.ClientSession() as session:
            res = await session.get(url)
            json = await res.json()
            pkmn_id = pokemon[np.where(pokemon[:, 0] == json["name"])][0][1]
            for m in json["moves"]:
                move_id = moves[np.where(moves[:, 0] == m["move"]["name"])][0][1]
                row = [pkmn_id, move_id]
                df.loc[len(df.index)] = row
    return get_movepool


async def import_pokeapi(url, columns, table, get_fn):
    async with aiohttp.ClientSession() as session:
        # Getting Pokémon in batches of 20 at a time.
        next = url
        offset = 0
        while next != None:
            df = DataFrame(columns=columns)
            res = await session.get(next)
            json = await res.json()
            print(f"appending {table}: {offset} - {len(json['results']) + offset - 1}")
            async with asyncio.TaskGroup() as tg:
                for val in json["results"]:
                    tg.create_task(get_fn(val["url"], df))
            df = df.drop_duplicates()
            await append_df(table, df)
            next = json["next"]
            offset += 20


async def main():
    # await import_pokeapi(
    #     "https://pokeapi.co/api/v2/pokemon",
    #     [
    #         "dex_no",
    #         "name",
    #         "primary_type",
    #         "secondary_type",
    #         "base_hp",
    #         "base_attack",
    #         "base_defense",
    #         "base_sp_attack",
    #         "base_sp_defense",
    #         "base_speed",
    #         "generation",
    #     ],
    #     "pokemon_info",
    #     get_pokemon_info,
    # )
    # await import_pokeapi(
    #     "https://pokeapi.co/api/v2/move",
    #     ["name", "type", "damage_class", "power", "accuracy", "pp", "priority"],
    #     "move_info",
    #     get_move_info,
    # )
    fn = await pre_process_movepool()
    await import_pokeapi(
        "https://pokeapi.co/api/v2/pokemon",
        ["pokemon_info_id", "move_id"],
        "move_pool",
        fn,
    )


asyncio.run(main())
