import os
import re
import aiohttp, asyncio
import time, datetime
import pandas as pd
import numpy as np
from multiprocessing import Process
from typing import Dict, List
from bs4 import BeautifulSoup
from pandas import DataFrame
import requests
import concurrent.futures
from utils.db_connect import db_connect
from concurrent.futures import ProcessPoolExecutor

NUM_CORES = os.cpu_count()


def process_folders(folders: List[str]):
    print(f"{len(folders)}")


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
    for l in soup.find_all("a"):
        href = l.get("href")
        if href != "../":
            if reg == None:
                urls.append(f"{url}{href}")
            elif re.match(reg, href):
                urls.append(f"{url}{href}")
    return urls


def main():
    smogon_url = "https://www.smogon.com/stats/"
    folders = get_links(smogon_url)
    futures = []
    chunks_size = round(len(folders) / NUM_CORES)
    # there's so much data being processed in abilities; that it's
    # best to use multiple CPU cores. since each process is working
    # independently and managing multiple downloads at once; it shouldn't
    # present any unique issues.
    with concurrent.futures.ProcessPoolExecutor(NUM_CORES) as executor:
        for i in range(NUM_CORES):
            # splitting the folders roughly among our various processes.
            start = i * chunks_size
            end = (i + 1) * chunks_size if i < NUM_CORES - 1 else len(folders) - 1
            new_future = executor.submit(process_folders, folders=folders[start:end])
            futures.append(new_future)
    concurrent.futures.wait(futures)


if __name__ == "__main__":
    main()
