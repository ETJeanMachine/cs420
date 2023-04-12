from datetime import date, timedelta
import aiohttp, asyncio, asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pandas import DataFrame
from db_connect import db_connect

async def main():
    conn = await db_connect()
    await conn.execute("""set search_path = "p42002_03";""")
    query_all = """
select p.name, m.month, sum(ps.raw_count) / t.count percentage
from pokemon_info p,
	 pokemon_stats ps,
	 metagame_info m
		 left join (select month, sum(total_battles)::float count
					from metagame_info m2
					where m2.cutoff = 0
					group by month) t on t.month = m.month
where p.pokemon_info_id = ps.pokemon_info_id
  and m.metagame_id = ps.metagame_id
  and m.cutoff = 0
group by m.month, p.name, t.count;"""
    query_top_5 = """
        select p.name
from pokemon_stats ps,
	 pokemon_info p,
	 metagame_info m
where ps.pokemon_info_id = p.pokemon_info_id
  and m.metagame_id = ps.metagame_id
  and m.cutoff = 0
group by p.name
order by sum(ps.raw_count) desc
limit 5;"""
    all_pokemon = await conn.fetch(query_all)
    top_5 = await conn.fetch(query_top_5)
    conn.close()
    df = DataFrame.from_records(all_pokemon)
    pokemon = df[0].unique()
    top_5_pokemon = DataFrame.from_records(top_5)[0].unique()
    fig, ax = plt.subplots(figsize=(10, 4), dpi=150, layout='compressed')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    max_y = df[2].max()
    for p in pokemon:
        vals = df[df[0] == p]
        if p not in top_5_pokemon:
            ax.plot(vals[1], vals[2], c="grey", alpha=0.5, linewidth=0.1)
    for i in range(len(top_5_pokemon) - 1, -1, -1):
        p = top_5_pokemon[i]
        vals = df[df[0] == p]
        match i:
            case 0:
                color = "gold"
            case 1:
                color = "silver"
            case 2:
                color = "saddlebrown"
            case 3:
                color = "darkcyan"
            case 4:
                color = "darkolivegreen"
            case _:
                color = None
        ax.plot(vals[1], vals[2], c=color)
        final = vals[vals[1] == vals[1].max()]
        final_x, final_y = final[1].values[0], final[2].values[0]
        x, y = [final_x, final_x + timedelta(weeks=4), final_x + timedelta(weeks=8)], [
            final_y,
            max_y - ((max_y / 5) * i),
            max_y - ((max_y / 5) * i),
        ]
        ax.plot(x, y, c=color, ls="--")
        ax.text(
            final_x + timedelta(weeks=8),
            max_y - ((max_y / 5) * i),
            f"#{i + 1}: {p.replace('-', ' ').title()}",
            fontsize='x-small',
            verticalalignment='center'
        )
    ax.set_ylabel("Usage")
    ax.set_xlabel("Time")
    # Plotting individual game releases against the graph.
    game_releases = {
        "Sun & Moon": date(2016, 11, 18),
        "Ultra Sun & Moon": date(2017, 11, 17),
        "Sword & Shield": date(2019, 11, 15),
        "Crown Tundra (DLC)": date(2020, 10, 22),
        "Scarlet & Violet": date(2022, 11, 18),
    }
    for k, v in game_releases.items():
        ax.axvline(x=v, c="black", ls="--", linewidth=0.5)
        ax.text(
            x=v, y=max_y + 0.01, s=k, fontsize="xx-small", horizontalalignment="center"
        )
    plt.autoscale(tight=True)
    fig.savefig('usageovertime.png', dpi=300)


asyncio.run(main())
