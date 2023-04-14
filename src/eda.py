from datetime import date, timedelta
import asyncio
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pandas import DataFrame
from utils.db_connect import connect

matplotlib.use("Agg")


async def eric_eda():
    conn = await connect()
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
    fig, ax = plt.subplots(figsize=(10, 4), dpi=150, layout="compressed")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
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
            fontsize="x-small",
            verticalalignment="center",
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
    fig.savefig("usageovertime.png", dpi=300)


async def drake_eda():
    conn = await connect
    moves_vs_usage_query = "SELECT \
            move_query.pokemon_info_id, move_query.name, move_query.moves_available, usage_query.percent_used \
            FROM (SELECT ps.pokemon_info_id, p.name, SUM(ps.raw_count::float) / SUM(mg.total_battles) AS percent_used \
          FROM pokemon_info p, \
               pokemon_stats ps, \
               metagame_info mg \
          WHERE mg.cutoff = 0 \
            AND ps.metagame_id = mg.metagame_id \
            AND p.pokemon_info_id = ps.pokemon_info_id \
          GROUP BY ps.pokemon_info_id, p.name \
          ORDER BY percent_used DESC) usage_query, \
         (SELECT m.pokemon_info_id, p.name, COUNT(DISTINCT m.move_id) AS moves_available \
          FROM pokemon_info p, \
               move_pool m \
          WHERE p.pokemon_info_id = m.pokemon_info_id \
          GROUP BY m.pokemon_info_id, p.name \
          ORDER BY COUNT(m.move_id) DESC) move_query \
            WHERE move_query.pokemon_info_id = usage_query.pokemon_info_id \
            ORDER BY percent_used DESC;"
    # This query intentionally double counts pokemon with multiple types so they can apply to both types
    type_vs_usage_query = "(SELECT ps.pokemon_info_id, p.name, p.primary_type AS type,  \
                              SUM(ps.raw_count::float) / SUM(mg.total_battles) AS percent_used \
      FROM pokemon_info p, \
           pokemon_stats ps, \
           metagame_info mg \
      WHERE mg.cutoff = 0 \
        AND ps.metagame_id = mg.metagame_id \
        AND p.pokemon_info_id = ps.pokemon_info_id \
      GROUP BY ps.pokemon_info_id, p.name, p.primary_type, p.secondary_type \
      ORDER BY percent_used DESC) \
    UNION ALL \
    SELECT ps.pokemon_info_id, p.name, p.secondary_type AS type, SUM(ps.raw_count::float)  \
                              / SUM(mg.total_battles) AS percent_used \
      FROM pokemon_info p, \
           pokemon_stats ps, \
           metagame_info mg \
      WHERE mg.cutoff = 0 \
        AND ps.metagame_id = mg.metagame_id \
        AND p.pokemon_info_id = ps.pokemon_info_id \
        AND p.secondary_type IS NOT NULL \
      GROUP BY ps.pokemon_info_id, p.name, p.primary_type, p.secondary_type ORDER BY percent_used DESC;"
    # Column 0 is pokemon id, 1 is pokemon name, 2 is move pool, 3 is usage of pokemon
    moves_vs_usage_res = await conn.fetch(moves_vs_usage_query)
    moves_vs_usage = DataFrame(moves_vs_usage_res).drop(columns=[0])
    moves_vs_usage.columns = ["name", "available_moves", "usage_percent"]
    type_vs_usage_res = await conn.fetch(type_vs_usage_query)
    type_vs_usage = DataFrame(type_vs_usage_res).drop(columns=[0])
    type_vs_usage.columns = ["name", "type", "usage_percent"]
    conn.close()
    p1 = sns.scatterplot(
        data=moves_vs_usage[["available_moves", "usage_percent"]],
        x="available_moves",
        y="usage_percent",
    )
    plt.title("Available Moves vs Popularity")
    plt.xlabel("Available Moves")
    plt.ylabel("Usage Percent")
    plt.savefig("moves_vs_popularity")
    # Add labels to plotted points
    for line in range(0, moves_vs_usage.shape[0]):
        p1.text(
            moves_vs_usage.available_moves[line] + 0.01,
            moves_vs_usage.usage_percent[line] + 0.01,
            moves_vs_usage.name[line],
            horizontalalignment="left",
            size="x-small",
            color="black",
            weight="medium",
        )
    plt.savefig("moves_vs_popularity_labeled")
    # Reset figure to do type
    plt.figure()
    mean_order = (
        type_vs_usage.groupby(["type"])["usage_percent"]
        .aggregate(np.mean)
        .reset_index()
        .sort_values("usage_percent", ascending=False)
    )
    p2 = sns.barplot(
        data=type_vs_usage, y="type", x="usage_percent", order=mean_order["type"]
    )
    plt.title("Popularity by Type")
    plt.xlabel("Usage Percent")
    plt.ylabel("Type")
    plt.savefig("popularity_by_type")


async def peter_eda():
    # Type hex codes
    BUG = "#9ACD32"
    DARK = "#000000"
    DRAGON = "#8A2BE2"
    ELECTRIC = "#FFD700"
    FAIRY = "#FFB6C1"
    FIGHTING = "#8B0000"
    FIRE = "#FF8C00"
    FLYING = "#00BFFF"
    GHOST = "#4B0082"
    GRASS = "#32CD32"
    GROUND = "#CD853F"
    ICE = "#B0E0E6"
    NORMAL = "#BDB76B"
    POISON = "#8B008B"
    PSYCHIC = "#FF1493"
    ROCK = "#A0522D"
    STEEL = "#808080"
    WATER = "#0000FF"
    conn = await connect()
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
    conn.close()

    type_colors = [
        NORMAL,
        ELECTRIC,
        STEEL,
        GHOST,
        PSYCHIC,
        WATER,
        ROCK,
        FAIRY,
        DRAGON,
        FIRE,
        DARK,
        GRASS,
        FIGHTING,
        FLYING,
        ICE,
        POISON,
        GROUND,
        BUG,
    ]
    sns.set(rc={"figure.figsize": (10, 5)})
    types = [
        "normal",
        "fire",
        "fighting",
        "water",
        "flying",
        "grass",
        "poison",
        "electric",
        "ground",
        "psychic",
        "rock",
        "ice",
        "bug",
        "dragon",
        "ghost",
        "dark",
        "steel",
        "fairy",
    ]
    damages = [
        250,
        180,
        150,
        195,
        140,
        150,
        120,
        210,
        120,
        200,
        190,
        140,
        120,
        185,
        200,
        180,
        200,
        190,
    ]
    damage_by_type = DataFrame(types)
    damage_by_type["power"] = damages
    damage_by_type.columns = ["type", "power"]
    damage_by_type.sort_values(by=["power"], inplace=True, ascending=False)
    print(damage_by_type)
    # mean_order = popularity_by_egg_group.groupby(['eg.name'])['percent_used'].aggregate(np.mean).reset_index().sort_values('percent_used', ascending=False)
    # p2 = sns.barplot(data=popularity_by_egg_group, y='eg.name', x='percent_used', order=mean_order['eg.name'], palette=custom_colors)
    # plt.title('Popularity by Egg Group')
    # plt.xlabel('Usage Percent')
    # plt.ylabel('Egg Group')
    # plt.savefig('popularity_by_egg_group')

    p1 = sns.barplot(
        data=damage_by_type,
        y="type",
        x="power",
        order=damage_by_type["type"],
        palette=type_colors,
    )
    # p1 = sns.histplot(data=damage_by_type, y='type', x='power', palette=type_colors)
    plt.title("Max Damage by Type")
    plt.xlabel("Max Damage")
    plt.ylabel("Type")
    plt.savefig("damage_by_type")


async def main():
    await eric_eda()
    # await drake_eda()
    # await peter_eda()


asyncio.run(main())
