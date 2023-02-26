import asyncio
import asyncpg
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import roman

matplotlib.use('Agg')
import seaborn as sns

from sshtunnel import SSHTunnelForwarder
from pandas import DataFrame

db_conn_file = open("./db_conn.key")
db_name = db_conn_file.readline().strip()
username = db_conn_file.readline().strip()
password = db_conn_file.readline().strip()

# Respective constants for the length of the regional dex of each generation
dex_sizes = {151, 100, 135, 107, 156, 72, 88, 96, 103}

def determine_generation(generation:str) -> int:
  return roman.fromRoman(generation.upper())

def determine_percentage(generation: str, dex_no: int) -> float:
  return sum(dex_sizes[0:determine_generation(generation.split('-', 1)[1]) - 1]) / dex_no
  

async def main():
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

        popularity_by_pokedex_no_query = "select pk.dex_no, pk.name, SUM(ps.raw_count::float) / SUM(mg.total_battles) as percent_used \
    	from pokemon_info pk, \
        	pokemon_stats ps, \
         	metagame_info mg \
			where mg.cutoff = 0 \
			and ps.metagame_id = mg.metagame_id \
			and pk.pokemon_info_id = ps.pokemon_info_id \
			group by pk.dex_no, pk.name, pk.generation \
			order by percent_used desc;"

        # Column 0 is pokedex number, 1 is pokemon name, 2 is percent used, 3 is the percentage of that pokemon in their regional pokedex
        popularity_by_pokedex_no_res = await conn.fetch(popularity_by_pokedex_no_query)
        pokedex_popularity = DataFrame(popularity_by_pokedex_no_res)
        pokedex_popularity.columns = ['dex_no', 'name', 'generation', 'percent_used', 'percentage_in_pokedex']
        for i in  pokedex_popularity.index:
          pokedex_popularity['percentage_in_pokedex'][i] = determine_percentage(pokedex_popularity['generation'][i], pokedex_popularity['dex_no'][i])

        try:
            p1 = sns.scatterplot(data=pokedex_popularity[['percentage_in_pokedex', 'percent_used']],
                                 x='percentage_in_pokedex', y='percent_used')
            plt.title('Pokemon Usage Percent vs Percentage of Pokedex Placement')
            plt.xlabel('Usage Percent')
            plt.ylabel('Percentage of Pokedex Placement')

            plt.savefig('pokedex_popularity')

            # Add labels to plotted points
            for line in range(0, pokedex_popularity.shape[0]):
                p1.text(pokedex_popularity.percentage_in_pokedex[line] + 0.01, pokedex_popularity.percent_used[line] + 0.01,
                        pokedex_popularity.name[line], horizontalalignment='left', size='x-small', color='black',
                        weight='medium')

            plt.savefig('hypothesis_4_labeled')

            # # Reset figure to do type
            # plt.figure()

            # mean_order = type_vs_usage.groupby(["type"])['usage_percent'].aggregate(np.mean).reset_index().sort_values('usage_percent', ascending=False)

        finally:
            conn.close()


asyncio.run(main())
