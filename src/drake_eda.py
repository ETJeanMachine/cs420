import asyncio

import asyncpg
import matplotlib
from sshtunnel import SSHTunnelForwarder
from pandas import DataFrame
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

matplotlib.use('Agg')
import seaborn as sns

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
        moves_vs_usage.columns = ['name', 'available_moves', 'usage_percent']

        type_vs_usage_res = await conn.fetch(type_vs_usage_query)
        type_vs_usage = DataFrame(type_vs_usage_res).drop(columns=[0])
        type_vs_usage.columns = ['name', 'type', 'usage_percent']

        try:
            p1 = sns.scatterplot(data=moves_vs_usage[['available_moves', 'usage_percent']],
                                 x='available_moves', y='usage_percent')
            plt.title('Available Moves vs Popularity')
            plt.xlabel('Available Moves')
            plt.ylabel('Usage Percent')

            plt.savefig('moves_vs_popularity')

            # Add labels to plotted points
            for line in range(0, moves_vs_usage.shape[0]):
                p1.text(moves_vs_usage.available_moves[line] + 0.01, moves_vs_usage.usage_percent[line] + 0.01,
                        moves_vs_usage.name[line], horizontalalignment='left', size='x-small', color='black',
                        weight='medium')

            plt.savefig('moves_vs_popularity_labeled')

            # Reset figure to do type
            plt.figure()

            mean_order = type_vs_usage.groupby(["type"])['usage_percent'].aggregate(np.mean).reset_index().sort_values('usage_percent', ascending=False)

            p2 = sns.barplot(data=type_vs_usage, y='type', x='usage_percent', order=mean_order['type'])

            plt.title('Popularity by Type')
            plt.xlabel('Usage Percent')
            plt.ylabel('Type')

            plt.savefig('popularity_by_type')

        finally:
            conn.close()


asyncio.run(main())
