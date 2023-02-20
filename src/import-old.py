import re
import aiohttp, asyncio, requests
import time, datetime
import pandas as pd
import numpy as np
import curses
from pathlib import Path
from pandas import DataFrame
from uuid import uuid4
from typing import List
from bs4 import BeautifulSoup

stdscr = curses.initscr()
curses.start_color()
curses.use_default_colors()
curses.init_pair(1, curses.COLOR_CYAN, -1)
curses.noecho()
curses.cbreak()
curses.curs_set(0)


def parse_urls(url: str, reg=None) -> List[str]:
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    urls = []
    for l in soup.find_all("a"):
        link = l.get("href")
        if link != "../":
            if reg == None:
                urls.append(link)
            elif re.match(reg, link):
                urls.append(link)
    return urls


async def read_data(
    data_url: str,
    df_list: List[DataFrame],
    session: aiohttp.ClientSession,
    yr,
    mn,
    n,
    y_pos,
    s_mn,
):
    date_time = datetime.datetime(yr, mn, 1)
    split = data_url.split("/")
    stdscr.addstr(y_pos + 2, 0, f"Loading {split[len(split) - 1]}{' '*40}\n")
    stdscr.refresh()
    try:
        res = await session.get(data_url)
        json_res = await res.json()
    except Exception as e:
        curses.nocbreak()
        stdscr.keypad(False)
        curses.echo()
        curses.endwin()
        raise e
    stdscr.addstr(y_pos + 2, 0, f"Transforming {split[len(split) - 1]}{' '*40}\n")
    stdscr.refresh()
    info, data = json_res["info"], json_res["data"]
    trans_data = []
    for name in data.keys():
        trans_data.append(
            {
                "Pokemon": name,
                "Month": date_time,
                "Metagame": info["metagame"],
                "Cutoff": info["cutoff"],
            }
        )
        for k in data[name].keys():
            trans_data[len(trans_data) - 1][k] = data[name][k]
    data_df = pd.json_normalize(trans_data, max_level=0)
    p1 = len(df_list) / n
    p2 = ((mn - s_mn) / (13 - s_mn)) + (p1 * (1 / (13 - s_mn)))
    stdscr.addstr(
        y_pos, 23, f"{'#' * int(p2 * 15)} {p2 * 100:0.1f}%\n", curses.color_pair(1)
    )
    stdscr.addstr(
        y_pos + 1,
        23,
        f"{'#' * int(p1 * 15)} {p1 * 100:0.1f}%\n",
        curses.color_pair(1),
    )
    stdscr.refresh()
    df_list.append(data_df)

async def main():
    url = "https://www.smogon.com/stats/"
    # total_rows, r_cnt, yr = 0, 0, 0
    curr_yr, y_pos, s_mn = -1, 0, -1
    t1 = time.perf_counter()
    async with aiohttp.ClientSession() as session:
        for month in parse_urls(url):
            # the chaos subfolder contains the full json data that we need. the txt files just mimic
            # what the json folder has; and is easier to process.
            chaos_url = f"{url}{month}chaos/"
            split = month.split("-")
            yr, mn = int(split[0]), int(split[1].replace("/", ""))
            if yr != curr_yr:
                t2 = time.perf_counter()
                s_mn = mn
                if t2 - t1 < 60:
                    c = f"{t2 - t1:06.3f}s"
                else:
                    min = int((t2 - t1) // 60)
                    c = f"{min}:{(t2 - t1) - (min * 60):04.1f}m"
                if curr_yr != -1:
                    stdscr.addstr(
                        y_pos, 0, f"Finished downloading {curr_yr} in {c}      \n"
                    )
                    y_pos += 1
                stdscr.addstr(
                    y_pos, 0, f"Downloading {yr}    =>   \n".ljust(stdscr.getmaxyx()[0])
                )
                curr_yr, t1 = yr, t2
            stdscr.addstr(y_pos + 1, 0, f"Downloading {yr}-{mn:02} =>   \n")
            stdscr.refresh()
            df_list: List[DataFrame] = []
            async with asyncio.TaskGroup() as tg:
                files = parse_urls(
                    chaos_url,
                    r"^(gen[5-9])?(doubles)?(ou|ubers|anythinggoes|vgc\d{4}(series\d)?)-\d+\.json$",
                )
                for data_file in files:
                    data_url = f"{chaos_url}{data_file}"
                    tg.create_task(
                        read_data(data_url, df_list, session, yr, mn, len(files), y_pos, s_mn)
                    )
            header = data_filepath.is_file()
            pd.concat(df_list).to_csv(data_filepath, mode='a+', header=header, index=False)
    curses.nocbreak()
    stdscr.keypad(False)
    curses.echo()
    curses.endwin()


asyncio.run(main())
