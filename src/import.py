import re
import asyncio
import aiohttp
import time
import requests
import pandas as pd
import numpy as np
from typing import List
from bs4 import BeautifulSoup


def parse_urls(url: str) -> List[str]:
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    urls = []
    for l in soup.find_all("a"):
        link = l.get("href")
        if link != "../":
            urls.append(link)
    return urls


async def read_data(data_url: str) -> int:
    # for now i'm just getting the json response for gen 7 data. what we can do from here is:
    # - create a csv which we individually process into data
    # - store this into a database, which we also construct in this python file
    # - process the list in some way after the fact.
    async with aiohttp.ClientSession() as session:
        json_res = await (await session.get(data_url)).json()
        info, data = json_res["info"], json_res["data"]
        trans_data = []
        for name in data.keys():
            trans_data.append({"Name": name})
            for k in data[name].keys():
                trans_data[len(trans_data) - 1][k] = data[name][k]
        data_df = pd.json_normalize(trans_data, max_level=0)
        return len(data_df.index)


def display_stats(r_cnt, total_rows, yr, t1, t2):
    if yr != 0:
        if t2 - t1 < 60:
            c = f"{t2 - t1:06.3f}s"
        else:
            min = int((t2 - t1) // 60)
            c = f"{min}:{(t2 - t1) - (min * 60):05.2f}m"
        print(f"Parsed {yr} in {c}: {r_cnt} rows counted ({total_rows} total).")


async def main():
    url = "https://www.smogon.com/stats/"
    month_links = parse_urls(url)
    total_rows = 0
    r_cnt = 0
    yr = 0
    t1 = time.perf_counter()
    for month in month_links:
        # the chaos subfolder contains the full json data that we need. the txt files just mimic
        # what the json folder has; and is easier to process.
        # NOTABLE: monotype has its own chaos folder. we can discuss at a later point if we care
        # to process monotype battle data.
        chaos_url = f"{url}{month}chaos/"
        curr_yr = int(month.split("-")[0])
        if curr_yr > yr:
            t2 = time.perf_counter()
            total_rows += r_cnt
            display_stats(r_cnt, total_rows, yr, t1, t2)
            t1, r_cnt, yr = t2, 0, curr_yr
        task_list = []
        async with asyncio.TaskGroup() as tg:
            for data_file in parse_urls(chaos_url):
                data_url = f"{chaos_url}{data_file}"
                if re.match(
                    r"^(gen[5-9])?(doubles)?(ou|ubers|anythinggoes|vgc\d{4})-\d+\.json$",
                    data_file,
                ):
                    task_list.append(tg.create_task(read_data(data_url)))
        for t in task_list:
            r_cnt += t.result()
    if r_cnt != 0:
        t2 = time.perf_counter()
        total_rows += r_cnt
        display_stats(r_cnt, total_rows, yr, t1, t2)


asyncio.run(main())
