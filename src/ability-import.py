import os
import re
import time
import aiohttp, asyncio
import datetime
from asyncpg import Connection
import asyncpg
import pandas as pd
import numpy as np
from typing import Dict, List
from bs4 import BeautifulSoup
from pandas import DataFrame
import requests
import concurrent.futures
import utils.db_connect as db

NUM_CORES = os.cpu_count()
TASK_COUNT = 5
FILTER = r"^(gen[5-9])?(doubles)?(ou|ubers|anythinggoes|vgc\d{4}(series\d)?)-\d+\.json$"


async def import_file(url: str):
    # one session per file.
    async with aiohttp.ClientSession() as session:
        response = await session.get(url)
        json = await response.json()
    info = json["info"]
    df = DataFrame.from_dict(json["data"], orient="index")
    abilities = df["Abilities"].copy()
    # Preserving memory.
    del df, json


async def chunk_worker(chunk: List[str]):
    for file in chunk:
        await import_file(file)


async def file_worker(files: List[str]):
    chunk_size = round(len(files) / TASK_COUNT)
    async with asyncio.TaskGroup() as tg:
        for i in range(TASK_COUNT):
            begin = i * chunk_size
            end = (i + 1) * chunk_size if i < TASK_COUNT - 1 else len(files)
            tg.create_task(chunk_worker(files[begin:end]))


def process_files(files: List[str], id: int):
    start = time.perf_counter()
    print(f"Process #{id + 1} started.")
    asyncio.run(file_worker(files))
    stop = time.perf_counter()
    print(
        f"Process #{id + 1} ended in {stop - start:0.3f}s. ({len(files)} files processed)"
    )


def get_links(url: str, reg: str = None) -> List[str]:
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
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    urls = []
    if reg == None:
        links = soup.find_all("a")
    else:
        links = soup.find_all("a", {"href": re.compile(reg)})
    for l in links:
        href = l.get("href")
        if href != "../":
            urls.append(f"{url}{href}")
    return urls


def query_helper(pokemon: str, iter):
    if iter == 1:
        return f"""SELECT pokemon_info_id FROM pokemon_info WHERE name LIKE'%{pokemon}%';"""
    elif iter > 2:
        # finding gendered pokemon.
        if re.match(r".*-(male|female|m|f)$", pokemon):
            gender = pokemon.split("-")[-1]
            # nidoran is special :3
            if "nidoran" in pokemon:
                if gender == "male":
                    pokemon = "nidoranm"
                else:
                    pokemon = "nidoranf"
            else:
                split = pokemon.split("-")
                pokemon = split[0]
                for i in range(len(split) - 2):
                    pokemon += f"-{split[i + 1]}"
                if gender == "female":
                    pokemon += "-f"
        else:
            # for all EXCEPT these, we just remove everything after the final -
            if "raticate" in pokemon or "marowak" in pokemon:
                split = pokemon.split("-")
                pokemon = split[0] + "-alola-totem"
            elif "zygarde" in pokemon:
                pokemon += "%"
            elif "basculin" in pokemon or "morpeko" in pokemon:
                split = pokemon.split("-")
                pokemon = split[0]
            else:
                split = pokemon.split("-")
                pokemon = split[0]
                for i in range(len(split) - 2):
                    pokemon += f"-{split[i + 1]}"
    return f"""SELECT pokemon_info_id FROM pokemon_info WHERE name = '{pokemon}';"""


async def add_ability_chunk(abilities):
    chunk = {}
    async with aiohttp.ClientSession() as session:
        for ability in abilities:
            response = await session.get(ability["url"])
            json = await response.json()
            pokemon = json["pokemon"]
            p_dict = {}
            for p in pokemon:
                p_dict[p["pokemon"]["name"]] = p["slot"]
            chunk[ability["name"]] = p_dict
    with db.tunnel() as server:
        conn = await db.connect(server)
        for ability in chunk.keys():
            for pokemon in chunk[ability]:
                if "walking-wake" not in pokemon or "iron-leaves" not in pokemon:
                    records, iter = "", 0
                    while len(records) != 1 and iter < 4:
                        query = query_helper(pokemon, iter)
                        records = await conn.fetch(query)
                        iter += 1
        conn.close()


async def add_abilities():
    async with aiohttp.ClientSession() as session:
        response = await session.get("https://pokeapi.co/api/v2/ability?limit=500")
        json = await response.json()
    results = json["results"]
    chunk_size = round(len(results) / TASK_COUNT)
    start = time.perf_counter()
    async with asyncio.TaskGroup() as tg:
        for i in range(TASK_COUNT):
            begin = i * chunk_size
            end = (i + 1) * chunk_size if i < TASK_COUNT - 1 else len(results)
            tg.create_task(add_ability_chunk(results[begin:end]))
    stop = time.perf_counter()
    runtime = time.strftime("%M:%S", time.gmtime(stop - start))
    print(f"Added all abilities in {runtime} minutes.")


def main():
    print("Getting smogon links...")
    start = time.perf_counter()
    smogon_url = "https://www.smogon.com/stats/"
    # we do have some regex here - this is for excluding data past January 2023.
    folders = get_links(smogon_url, r"^20(?!2[3-9]-[01][2-9])\d{2}-[01]\d.*/$")
    files = []
    for folder in folders:
        chaos = f"{folder}/chaos/"
        files.extend(get_links(chaos, FILTER))
    stop = time.perf_counter()
    print(f"Got {len(files)} file links in {stop - start:0.3f}s")
    futures = []
    chunk_size = round(len(files) / NUM_CORES)
    # there's so much data being processed in abilities; that it's
    # best to use multiple CPU cores. since each process is working
    # independently and managing multiple downloads at once; it shouldn't
    # present any unique issues.
    start = time.perf_counter()
    with concurrent.futures.ProcessPoolExecutor(NUM_CORES) as executor:
        for i in range(NUM_CORES):
            # splitting the folders roughly among our various processes.
            begin = i * chunk_size
            end = (i + 1) * chunk_size if i < NUM_CORES - 1 else len(files)
            new_future = executor.submit(process_files, files=files[begin:end], id=i)
            futures.append(new_future)
    concurrent.futures.wait(futures)
    stop = time.perf_counter()
    runtime = time.strftime("%M:%S", time.gmtime(stop - start))
    print(f"Processed all files in {runtime} minutes.")


if __name__ == "__main__":
    asyncio.run(add_abilities())
