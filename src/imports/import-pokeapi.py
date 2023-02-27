import re
import aiohttp, asyncio, requests, asyncpg
import pandas as pd
import numpy as np
from pandas import DataFrame
from sshtunnel import SSHTunnelForwarder

"""
ORDER OF IMPORTS:
1.  pokemon_info   - pokeapi [X]
2.  move_info      - pokeapi [X]
3.  move_pool      - pokeapi [X]
4.  egg_group      - pokeapi [X]
5.  metagame_info  - smogon  [X]
6.  pokemon_stats  - smogon  [X]
7.  move_stats     - smogon  [X]
8.  nature_stats   - smogon  [ ]
9.  ability_stats  - smogon  [ ]
10. teammate_stats - smogon  [ ]
11. item_stats     - smogon  [ ]
*may end up being last form import due to multi-to-multi reasons.
"""

# Getting valid information from the connection file.
db_conn_file = open("./db_conn.key")
db_name = db_conn_file.readline().strip()
username = db_conn_file.readline().strip()
password = db_conn_file.readline().strip()

"""
This entire file is done, and has served it's purpose. 
You can largely ignore it, and it's lack of comments. :p
"""

async def append_df(table: str, df: DataFrame, index=False):
    """Function appends the dataframe to the provided table.

    Args:
        table (str): The table in the database that's being appended to.

        df (DataFrame): The dataframe that is being appended to the table. 
        The names of the columns in the dataframe must match the names of the columns
        in the database.

        index (bool, optional): Whether or not to use the index of the dataframe when appending. Defaults to False.
    """
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
        records = df.itertuples(index=index, name=None)
        # appending the table.
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
            # dex number requires a separate API call
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


async def preprocess_move_pool():
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
        move_res = await conn.fetch(f"SELECT name, move_id FROM {db_name}.move_info")
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


async def get_egg_groups(url, df: DataFrame):
    async with aiohttp.ClientSession() as session:
        res = await session.get(url)
        json = await res.json()
        row = [json["name"], json["id"]]
        df.loc[len(df.index)] = row


async def preprocess_egg_groups():
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
        egg_res = await conn.fetch(
            f"SELECT name, egg_group_id FROM {db_name}.egg_group"
        )
        pokemon = np.array(pkmn_res)
        egg_groups = np.array(egg_res)

    async def get_egg_rel(url, df: DataFrame):
        async with aiohttp.ClientSession() as session:
            res = await session.get(url)
            json = await res.json()
            species_res = await session.get(json["species"]["url"])
            species_json = await species_res.json()
            pkmn_id = pokemon[np.where(pokemon[:, 0] == json["name"])][0][1]
            for e in species_json["egg_groups"]:
                egg_id = egg_groups[np.where(egg_groups[:, 0] == e["name"])][0][1]
                row = [pkmn_id, egg_id]
                df.loc[len(df.index)] = row

    return get_egg_rel


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
            # doing API calls in batches of 20.
            async with asyncio.TaskGroup() as tg:
                for val in json["results"]:
                    tg.create_task(get_fn(val["url"], df))
            # dropping identical rows
            df = df.drop_duplicates()
            # sending the stuff to the db
            await append_df(table, df)
            next = json["next"]
            offset += 20


async def main():

    pkmn_inf = (
        "https://pokeapi.co/api/v2/pokemon",
        [
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
        ],
        "pokemon_info",
        get_pokemon_info,
    )
    move_inf = (
        "https://pokeapi.co/api/v2/move",
        ["name", "type", "damage_class", "power", "accuracy", "pp", "priority"],
        "move_info",
        get_move_info,
    )
    mp_inf = (
        "https://pokeapi.co/api/v2/pokemon",
        ["pokemon_info_id", "move_id"],
        "move_pool",
        await preprocess_move_pool(),
    )
    egg_inf = (
        "https://pokeapi.co/api/v2/egg-group",
        ["name", "egg_group_id"],
        "egg_group",
        get_egg_groups,
    )
    egg_g_inf = (
        "https://pokeapi.co/api/v2/pokemon",
        ["pokemon_info_id", "egg_group_id"],
        "pokemoninfo_egggroup",
        await preprocess_egg_groups(),
    )
    v1, v2, v3, v4 = egg_g_inf[0], egg_g_inf[1], egg_g_inf[2], egg_g_inf[3]
    # await import_pokeapi(v1, v2, v3, v4)


asyncio.run(main())
