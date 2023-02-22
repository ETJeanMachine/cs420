import re
from typing import List
import aiohttp, asyncio, asyncpg
import time, datetime
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from pandas import DataFrame
from sshtunnel import SSHTunnelForwarder


# Getting valid information from the connection file.
db_conn_file = open("./db_conn.key")
db_name = db_conn_file.readline().strip()
username = db_conn_file.readline().strip()
password = db_conn_file.readline().strip()

# global increments we do in code ourselves for reference handling
meta_id, stats_id = 0, 0
outliers = set()  # for debugging.


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


async def get_links(url: str, reg: str = None) -> List[str]:
    """Helper function that parses the html of the stats page
    and returns the list of url links that can be seen on it.

    Args:
        url (str): The url we're scraping.
        reg (str, optional): The regular expression to filter out links on.
        Defaults to None.

    Returns:
        List[str]: The list of urls that we see on the webpage
        (excluding any backwards links/regex filtered links).
    """
    async with aiohttp.ClientSession() as session:
        res = await session.get(url)
        text = await res.text()
        soup = BeautifulSoup(text, "html.parser")
        urls = []
        for l in soup.find_all("a"):
            href = l.get("href")
            if href != "../":
                if reg == None:
                    urls.append(f"{url}{href}")
                elif re.match(reg, href):
                    urls.append(f"{url}{href}")
    return urls


async def add_data_files(url: str, month: datetime.date, data_files: List[str]):
    """Very, very basic helper function that adds to a list of data files all the data files
    within a specific month folder on smogon's site. Used for async programming.

    Args:
        url (str): The url string we're pulling data from.
        month (datetime.date): The month/year that this url is associated with.
        data_files (List[str]): The list of data files we're adding relevant info to.
    """
    reg = (
        r"^(gen[5-9])?(doubles)?(ou|ubers|anythinggoes|vgc\d{4}(series\d)?)-\d+\.json$"
    )
    files = await get_links(url, reg)
    for f in files:
        # The data files list is in the format [[url, month] ...]
        data_files.append([f, month])


async def get_smogon_data(
    url: str, month: datetime.date, pokemon: DataFrame, moves: DataFrame
):
    """This is the core function of the program. This takes in a url to a json file,
    gathers the data from it, transforms it, and calls `append_df` to add all of the
    info to the database.

    Args:
        url (str): The link to the json file we're reading.
        month (datetime.date): The month that this json file is associated with.
        pokemon (DataFrame): A dataframe that's equal to the `pokemon_info` table
        in the database.
        moves (DataFrame): A dataframe that's equal to the `move_info` table
        in the database.
    """
    # global values we need to be constantly updating
    global meta_id, stats_id
    async with aiohttp.ClientSession() as session:
        # getting the json data
        res = await session.get(url)
        json = await res.json()
        meta_inf = json["info"]
        # generating a single dataframe of metagame info. this is basically just a single row of the table.
        meta_df = DataFrame(
            index={"metagame_id": meta_id},
            data={
                "metagame_name": meta_inf["metagame"],
                "cutoff": meta_inf["cutoff"],
                "datetime": month,
                "total_battles": meta_inf["number of battles"],
            },
        )
        meta_id += 1
        # await append_df("metagame_info", meta_df, True) # this is commented out for now as to not store info prematurely. it works.
        data = DataFrame()
        # this creates a dataframe with indices as pokemon names, and the rest the values that smogon has (which will become tables
        # of their own)
        data = data.from_dict(json["data"], orient="index")
        data.index = (
            data.index.str.lower().str.replace(" ", "-").str.replace(r"\.|\'", "")
        )
        # This here is how i figure out which pokemon arent in pokeapi, but are in smogon.
        merged = data.merge(
            pokemon, how="left", left_index=True, right_index=True, indicator=True
        )
        not_in = merged.loc[merged["_merge"] == "left_only"]
        outliers.update(not_in.index.to_list())


async def main():
    """
    The main function basically handles the "asynchronous" aspect of the program, as to
    make it run significantly faster.
    """
    smogon_url = "https://www.smogon.com/stats/"
    # gets all the folders that are within the main `stats` page.
    folders = await get_links(smogon_url)
    data_files = []

    # everything within the "with" here is just to gather the `move_info` and `pokemon_info`
    # tables into dataframes that we use later in the program.
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
        # defining the moves and pokemon dataframes correctly; after we get their info from the db.
        pokemon = DataFrame(pkmn_res)
        moves = DataFrame(move_res)
        pokemon.index = pokemon[0]
        moves.index = moves[0]
        pokemon = pokemon.drop(columns=[0])
        moves = moves.drop(columns=[0])

    # an async taskgroup that gathers all of the json files that we need. `data_files`
    # is populated after this is run (in the form [[url, month], ...]).
    async with asyncio.TaskGroup() as tg:
        for folder in folders:
            data_url = f"{folder}chaos/"
            split_url = folder.split("/")
            split_val = split_url[len(split_url) - 2].split("-")
            month = datetime.date(int(split_val[0]), int(split_val[1]), 1)
            tg.create_task(add_data_files(data_url, month, data_files))
    # this is for handling async groups (does 100 json files async at once rn)
    offset, inc = 0, 100
    # this is for just printing how long each batch takes and estimating remaining time.
    times = []
    # THE MAIN LOOP; GOES THROUGH ALL DATA FILES.
    while offset < len(data_files):
        end = min(offset + inc, len(data_files))
        chunk = data_files[offset:end]
        t = time.perf_counter()
        # This taskgroup creates tasks for every batch of `inc` (currently 100) json files.
        # The `async with ...` syntax prevents any further code from computing until all the
        # files have been processed.
        async with asyncio.TaskGroup() as tg:
            for info in chunk:
                tg.create_task(get_smogon_data(info[0], info[1], pokemon, moves))
        # This just prints out to the console how long things took/look like they'll take to do.
        times.append(time.perf_counter() - t)
        est = (np.mean(times) * (len(data_files) / inc)) - sum(times)
        fmt_est = time.strftime("%M:%S", time.gmtime(est))
        print(
            f"Managed {end}/{len(data_files)} json files in {times[len(times)-1]:06.3f}s (~{fmt_est}m remaining)"
        )
        # Setting the offset
        offset += inc
    # Printing how long everything took.
    total_time = time.strftime("%M:%S", time.gmtime(sum(times)))
    print(f"Parsed all files in {total_time}m")
    print(outliers) # debugging problem children.


# Required for asyncio to work.
asyncio.run(main())
