"""Silver star schema (Fabric: nb_03_4_teams_players_star_silver / ``games_teams_star``).

Materialized views: Databricks refreshes them (incrementally where it can) whenever their
silver inputs change, so there are no explicit write steps.
"""
from pyspark import pipelines as dp
from utilities import star_logic as sl
from utilities.config import silver


def _game():
    return sl.with_season_start_year(spark.read.table(silver("game")))  # noqa: F821


@dp.materialized_view(name=silver("dim_team"))
def dim_team():
    return sl.build_dim_team(spark.read.table(silver("team_info")))  # noqa: F821


@dp.materialized_view(name=silver("fact_team_game"))
def fact_team_game():
    return sl.build_fact_team_game(_game(), spark.read.table(silver("game_teams_stats")))  # noqa: F821


@dp.materialized_view(name=silver("fact_team_champion"))
def fact_team_champion():
    return sl.build_fact_team_champion(_game())


@dp.materialized_view(name=silver("fact_skater_game"))
def fact_skater_game():
    return sl.build_fact_skater_game(_game(), spark.read.table(silver("game_skater_stats")))  # noqa: F821


@dp.materialized_view(name=silver("fact_goalie_game"))
def fact_goalie_game():
    return sl.build_fact_goalie_game(_game(), spark.read.table(silver("game_goalie_stats")))  # noqa: F821
