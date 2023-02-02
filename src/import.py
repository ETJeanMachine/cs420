import re
import json
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


def main():
    url = "https://www.smogon.com/stats/"
    month_links = parse_urls(url)
    gen_7_data = []
    for month in month_links:
        # the chaos subfolder contains the full json data that we need. the txt files just mimic
        # what the json folder has; and is easier to process.
        # NOTABLE: monotype has its own chaos folder. we can discuss at a later point if we care
        # to process monotype battle data.
        chaos_url = f"{url}{month}chaos/"
        for data_file in parse_urls(chaos_url):
            data_url = f"{chaos_url}{data_file}"
            if re.match(r"^gen7.*\.json$", data_file):
                # for now i'm just getting the json response for gen 7 data. what we can do from here is:
                # - create a csv which we individually process into data
                # - store this into a database, which we also construct in this python file
                # - process the list in some way after the fact.
                json_res = requests.get(data_url).json()
                info, data = json_res["info"], json_res["data"]
                trans_data = []
                for name in data.keys():
                    trans_data.append({"Name": name})
                    for k in data[name].keys():
                        trans_data[len(trans_data) - 1][k] = data[name][k]
                data_df = pd.json_normalize(trans_data, max_level=0)
                gen_7_data.append(data_df)
    print(gen_7_data)


if __name__ == "__main__":
    main()
