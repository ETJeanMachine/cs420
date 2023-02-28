import aiohttp, asyncio, asyncpg
import pandas as pd
from pandas import DataFrame
from asyncpg.connection import Connection
from asyncpg import Record
from sshtunnel import SSHTunnelForwarder

db_conn_file = open("./db_conn.key")
db_name = db_conn_file.readline().strip()
username = db_conn_file.readline().strip()
password = db_conn_file.readline().strip()


async def get_pokemon_info():
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
        await conn.execute("""set search_path = "p42002_03";""")
        records = await conn.fetch(
            """select pokemon_info_id, name, dex_no, is_primary, is_mythical, is_legendary
               from pokemon_info
               order by dex_no;"""
        )
        df = DataFrame.from_records(
            records,
            columns=[
                "pokemon_info_id",
                "name",
                "dex_no",
                "is_primary",
                "is_mythical",
                "is_legendary",
            ],
        )
        conn.close()
    return df


async def correct_dex_no(url: str, pkmn_info: DataFrame):
    async with aiohttp.ClientSession() as session:
        res = await session.get(url)
        json = await res.json()
        order = json["order"]
        dex_no = json["pokedex_numbers"][0]["entry_number"]
        is_mythical = json["is_mythical"]
        is_legendary = json["is_legendary"]
        our_forms = pkmn_info[pkmn_info["old_dex_no"] == order].index
        their_forms = json["varieties"]
        if (
            order == dex_no
            and not is_mythical
            and not is_legendary
            and len(our_forms) == 1
        ):
            pkmn_info.drop(our_forms, inplace=True)
        else:
            if json['name'] == 'urshifu':
                pass
            if len(our_forms) > 1:
                pkmn_info.loc[
                    (pkmn_info["old_dex_no"] == order)
                    & ~(pkmn_info["name"] == json["name"]),
                    ["is_primary"],
                ] = False
                if len(pkmn_info[(pkmn_info['old_dex_no'] == order) & (pkmn_info['is_primary'])]) != 1:
                    for v in their_forms:
                        if v["is_default"]:
                            name = v["pokemon"]["name"]
                            pkmn_info.loc[
                                (pkmn_info["old_dex_no"] == order)
                                & ~(pkmn_info["name"] == name),
                                ["is_primary"],
                            ] = False
                            break
            pkmn_info.loc[our_forms, ["dex_no", "is_mythical", "is_legendary"]] = [
                dex_no,
                is_mythical,
                is_legendary,
            ]


async def send_update(old_no, new_no):
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
        await conn.execute("""set search_path = "p42002_03";""")
        update = f"""
        UPDATE pokemon_info p
        SET 
        """
        conn.close()


async def main():
    pkmn_info = await get_pokemon_info()
    pkmn_info["old_dex_no"] = pkmn_info["dex_no"]
    next = "https://pokeapi.co/api/v2/pokemon-species?offset=0&limit=100"
    offset = 0
    # Getting corrected data and storing it into pkmn_info
    while next != None:
        async with aiohttp.ClientSession() as session:
            res = await session.get(next)
            json = await res.json()
            async with asyncio.TaskGroup() as tg:
                for val in json["results"]:
                    tg.create_task(correct_dex_no(val["url"], pkmn_info))
            next = json["next"]
            offset += 100
    offset = 0
    # Storing our updates to the database.
    # TODO


asyncio.run(main())
