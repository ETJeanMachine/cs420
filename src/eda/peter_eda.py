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

# Type hex codes
BUG = '#9ACD32'
DARK = '#000000'
DRAGON = '#8A2BE2'
ELECTRIC = '#FFD700'
FAIRY = '#FFB6C1'
FIGHTING = '#8B0000'
FIRE = '#FF8C00'
FLYING = '#00BFFF'
GHOST = '#4B0082'
GRASS = '#32CD32'
GROUND = '#CD853F'
ICE = '#B0E0E6'
NORMAL = '#BDB76B'
POISON = '#8B008B'
PSYCHIC = '#FF1493'
ROCK = '#A0522D'
STEEL = '#808080'
WATER = '#0000FF'

db_conn_file = open("./db_conn.key")
db_name = db_conn_file.readline().strip()
username = db_conn_file.readline().strip()
password = db_conn_file.readline().strip()

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

        try:
            # popularity_by_egg_group_query =  """
            # select pk.name, eg.name, pkie.pokemon_info_id, pkie.egg_group_id,
            # SUM(ps.raw_count::float) / SUM(mg.total_battles) as percent_used
            # from pokemon_info pk, egg_group eg, pokemoninfo_egggroup pkie, pokemon_stats ps, metagame_info mg
            # where mg.cutoff = 0 and ps.metagame_id = mg.metagame_id and pk.pokemon_info_id = ps.pokemon_info_id
            # and eg.egg_group_id = pkie.egg_group_id and pk.pokemon_info_id = pkie.pokemon_info_id
            # group by pk.name, eg.name, pkie.pokemon_info_id, pkie.egg_group_id, eg.name
            # order by percent_used desc;
            # """
            # damage_by_type_query = """
            # select mi.type, mi.power
            # from move_info mi
            # where mi.damage_class not like 'status% and power is not null'
            # order by power asc;
            # """

            # # Column 0 is pokedex number, 1 is pokemon name, 2 is percent used, 3 is the percentage of that pokemon in their regional pokedex
            # # popularity_by_egg_group_res = await conn.fetch(popularity_by_egg_group_query)
            # # popularity_by_egg_group = DataFrame(popularity_by_egg_group_res)
            # # popularity_by_egg_group.columns = ['pk.name', 'eg.name', 'pokemon_info_id', 'egg_info_id', 'percent_used']
            # damage_by_type_res = await conn.fetch(damage_by_type_query)
            # damage_by_type = DataFrame(damage_by_type_res)
            # damage_by_type.columns = ['type', 'power']

            # Custom colors for the egg groups. The order is: no-eggs, ditto, dragon, humanshape, monster, indeterminate, fairy, water1, 
            #                                                 plant, mineral, flying, ground, bug, water2, water3
            # custom_colors = ["#000000", "#A200FF", "#6200FF", "#C0C0C0", "#FF0000", "#8B0000", "#FFB6C1", "#1E90FF", "#008000", "#FFD700", "#00BFFF", 
            #                 "#A0522D", "#6B8E23", "#0000CD", "#00008B"]
            # sns.set_palette(sns.color_palette(custom_colors))

            # Order: Fire, Dragon, Steel, Fairy, Electric, Psychic, Ghost, Grass, Fighting, Rock, Flying, Water, Ice, Normal, Ground, Dark, Poison, Bug
            type_colors = [NORMAL, ELECTRIC, STEEL, GHOST, PSYCHIC, WATER, ROCK, FAIRY, DRAGON, FIRE, DARK, GRASS, FIGHTING, FLYING, ICE, POISON, GROUND, BUG]
            sns.set(rc={"figure.figsize":(10, 5)}) 

            types =['normal', 'fire', 'fighting', 'water', 'flying', 'grass', 'poison', 'electric', 'ground', 'psychic', 'rock', 'ice', 'bug', 'dragon',
                    'ghost', 'dark', 'steel', 'fairy']
            damages = [250, 180, 150, 195, 140, 150, 120, 210, 120, 200, 190, 140, 120, 185, 200, 180, 200, 190]
            damage_by_type = DataFrame(types)
            damage_by_type['power'] = damages
            damage_by_type.columns = ['type', 'power']
            damage_by_type.sort_values(by=['power'], inplace=True, ascending=False)
            print(damage_by_type)

            # mean_order = popularity_by_egg_group.groupby(['eg.name'])['percent_used'].aggregate(np.mean).reset_index().sort_values('percent_used', ascending=False)

            # p2 = sns.barplot(data=popularity_by_egg_group, y='eg.name', x='percent_used', order=mean_order['eg.name'], palette=custom_colors)

            # plt.title('Popularity by Egg Group')
            # plt.xlabel('Usage Percent')
            # plt.ylabel('Egg Group')

            # plt.savefig('popularity_by_egg_group')

            
            p1 = sns.barplot(data=damage_by_type, y='type', x='power', order=damage_by_type['type'], palette=type_colors)
            # p1 = sns.histplot(data=damage_by_type, y='type', x='power', palette=type_colors)
            plt.title('Max Damage by Type')
            plt.xlabel('Max Damage')
            plt.ylabel('Type')
            plt.savefig('damage_by_type')

        finally:
            conn.close()    

asyncio.run(main())