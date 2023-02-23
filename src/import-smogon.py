import re
from typing import Dict, List
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

# for debugging.
pkmn_outliers = set()
move_outliers = set()


async def append_df(table: str, df: DataFrame):
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
        records = df.itertuples(index=False, name=None)
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


async def extract_json(df: DataFrame, column: str, c1: str, c2: str, explode_on: str):
    m = (
        pd.DataFrame([*df[column]], df.index)
        .stack()
        .rename_axis([None, c1])
        .reset_index(1, name=c2)
    )
    return df[[explode_on]].join(m)


async def get_smogon_data(
    url: str,
    month: datetime.date,
    pokemon: DataFrame,
    moves: DataFrame,
    shift: Dict[str, int],
    dataframes: Dict[str, DataFrame],
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
    async with aiohttp.ClientSession() as session:
        # getting the json data
        res = await session.get(url)
        json = await res.json()
        # skipping empty data
        if json["data"] == {}:
            return
        meta_inf = json["info"]
        # generating a single dataframe of metagame info. this is basically just a single row of the table.
        meta_idx = len(dataframes["metagame_info"].index) + shift["metagame_info"]
        meta_df = DataFrame(
            columns=["metagame_name", "cutoff", "month", "total_battles"]
        )
        meta_df.loc[len(meta_df.index)] = [
            meta_inf["metagame"],
            meta_inf["cutoff"],
            month,
            meta_inf["number of battles"],
        ]
        dataframes["metagame_info"] = pd.concat(
            [dataframes["metagame_info"], meta_df], ignore_index=True
        )
        # this creates a dataframe with indices as pokemon names, and the rest the values that smogon has (which will become tables
        # of their own)
        data = DataFrame.from_dict(json["data"], orient="index")
        data.index = (
            data.index.str.lower().str.replace(" ", "-").str.replace(r"\.|\'", "")
        )
        # This here is how i figure out which pokemon arent in pokeapi, but are in smogon.
        merged = data.merge(pokemon, how="inner", left_index=True, right_index=True)
        merged.reset_index(inplace=True)
        merged = merged.rename(columns={"index": "name", 1: "id"})
        # This adds to `pokemon_stats`
        stats_df = DataFrame(
            columns=["pokemon_info_id", "metagame_id", "raw_count"],
        )
        stats_df["pokemon_info_id"] = merged["id"]
        stats_df["metagame_id"] = meta_idx
        stats_df["raw_count"] = merged["Raw count"]
        # getting outliers
        if len(stats_df.index) != len(data.index):
            not_in = data.merge(
                pokemon, how="left", left_index=True, right_index=True, indicator=True
            )
            not_in = not_in.loc[not_in["_merge"] == "left_only"]
            pkmn_outliers.update(not_in.index.to_list())
        # this shift allows us to figure out the id's of the stats (necessary for everything else)
        stats_idx_shift = (
            len(dataframes["pokemon_stats"].index) + shift["pokemon_stats"]
        )
        dataframes["pokemon_stats"] = pd.concat(
            [dataframes["pokemon_stats"], stats_df], ignore_index=True
        )
        # adding to `move_stats`
        moves_data = DataFrame(merged["Moves"])
        moves_data.index += stats_idx_shift
        moves_data.reset_index(names="stats_id", inplace=True)
        # FANCY PIVOTING TO GET JSON
        moves_data = await extract_json(
            moves_data,
            "Moves",
            "name",
            "move_usage",
            "stats_id",
        )
        # FANCY PIVOTING TO GET JSON
        moves_df = moves_data.merge(moves, left_on="name", right_index=True)
        moves_df.drop(columns=["name"], inplace=True)
        moves_df.rename(columns={1: "move_id"}, inplace=True)
        moves_df.reset_index(drop=True, inplace=True)
        # getting move outliers (there are some).
        if len(moves_df.index) != len(moves_data.index):
            not_in = moves_data.merge(
                moves, how="left", left_on="name", right_index=True, indicator=True
            )
            not_in = not_in.loc[not_in["_merge"] == "left_only"]
            move_outliers.update(not_in["name"].to_list())
        dataframes["move_stats"] = pd.concat(
            [dataframes["move_stats"], moves_df], ignore_index=True
        )
        # lmaooooooo
        if True == False:
            # acquiring (relevant) nature data.
            nature_data = DataFrame(merged["Spreads"])
            nature_data.index += stats_idx_shift
            nature_data.reset_index(names="stats_id", inplace=True)
            # testing chat-gpt bs
            # holy crap lois it worked
            explode_nature = pd.json_normalize(nature_data["Spreads"])
            explode_nature["stats_id"] = nature_data["stats_id"]
            grouped_nature = explode_nature.groupby(lambda x: x.split(":")[0], axis=1)
            nature_df = grouped_nature.agg("sum")
            # okay but not exactly gotta do some extra stuff
            nature_df.columns = nature_df.columns.str.lower()
            # ahHHHHHHHHHHHHH (long set)
            natures = {
                "hardy",
                "lonely",
                "brave",
                "adamant",
                "naughty",
                "bold",
                "docile",
                "relaxed",
                "impish",
                "lax",
                "timid",
                "hasty",
                "serious",
                "jolly",
                "naive",
                "modest",
                "mild",
                "quiet",
                "bashful",
                "rash",
                "calm",
                "gentle",
                "sassy",
                "careful",
                "quirky",
            }
            missing = natures.difference(set(nature_df.columns.to_list()))
            nature_df[list(missing)] = 0
            dataframes["nature_stats"] = pd.concat(
                [dataframes["nature_stats"], nature_df], ignore_index=True
            )
        pass


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
        pokemon, moves = DataFrame(pkmn_res), DataFrame(move_res)
        pokemon.index, moves.index = pokemon[0], moves[0]
        pokemon, moves = pokemon.drop(columns=[0]), moves.drop(columns=[0])
        # for move processing we have to remove the `-`.
        moves.index = moves.index.str.replace("-", "")
        conn.close()

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
    # Shift vals is for setting indices - while id names is the name of the indices in
    # the database.
    table_names = [
        "metagame_info",
        "pokemon_stats",
        "move_stats",
        # "nature_stats"
    ]
    shift_vals = {}
    for t in table_names:
        shift_vals[t] = 1
    id_name = {
        "metagame_info": "metagame_id",
        "pokemon_stats": "stats_id",
        "move_stats": "movedata_id",
        # "nature_stats": None,
    }
    # this is for just printing how long each batch takes and estimating remaining time.
    times = []
    n_pkmn_outliers = 0
    n_move_outliers = 0
    # THE MAIN LOOP; GOES THROUGH ALL DATA FILES.
    while offset < len(data_files):
        end = min(offset + inc, len(data_files))
        chunk = data_files[offset:end]
        t = time.perf_counter()
        # This taskgroup creates tasks for every batch of `inc` (currently 100) json files.
        # The `async with ...` syntax prevents any further code from computing until all the
        # files have been processed.
        data_dict: Dict[str, DataFrame] = {}
        # Creating empty table dicts of which we compress into defined tables later on.
        for tb in table_names:
            data_dict[tb] = DataFrame()
        async with asyncio.TaskGroup() as tg:
            for info in chunk:
                tg.create_task(
                    get_smogon_data(
                        info[0], info[1], pokemon, moves, shift_vals, data_dict
                    )
                )
        for k in data_dict.keys():
            # Shifting indices, changing the index to be the actual amount of processed data.
            df = data_dict[k]
            df.index += shift_vals[k]
            if id_name[k] != None:
                shift_vals[k] += len(df.index)
                df.reset_index(names=id_name[k], inplace=True)
            # We store in batches after EVERYTHING is done for the 100 json files.
            await append_df(k, df)
        if len(pkmn_outliers) != n_pkmn_outliers:
            print(f"{len(pkmn_outliers)} problem children seen so far.")
            n_pkmn_outliers = len(pkmn_outliers)
        if len(move_outliers) != n_move_outliers:
            print(f"{len(move_outliers)} problem moves seen so far.")
            n_move_outliers = len(move_outliers)
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
    print(pkmn_outliers)  # debugging problem children.
    print(move_outliers)


# Required for asyncio to work.
asyncio.run(main())
