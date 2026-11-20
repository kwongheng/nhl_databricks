"""Gold: season rollups, forecasts and championship odds
(Fabric: nb_04_teams_players_forecast_gold / ``teams_players_forecast``)."""
from pyspark import pipelines as dp
from utilities import gold_logic as gl
from utilities.config import gold, silver


def _read(name):
    return spark.read.table(name)  # noqa: F821


# ---- season rollups ------------------------------------------------------------------
@dp.materialized_view(name=gold("team_season"))
def team_season():
    return gl.build_team_season(
        _read(silver("fact_team_game")), _read(silver("fact_team_champion")), _read(silver("dim_team"))
    )


@dp.materialized_view(name=gold("skater_season"))
def skater_season():
    return gl.build_skater_season(_read(silver("fact_skater_game")), _read(silver("player_info")))


@dp.materialized_view(name=gold("goalie_season"))
def goalie_season():
    return gl.build_goalie_season(_read(silver("fact_goalie_game")), _read(silver("player_info")))


# ---- backtest (2009-2018) -------------------------------------------------------------
@dp.materialized_view(name=gold("champion_validation_history"))
def champion_validation_history():
    return gl.build_champion_history(
        _read(silver("dim_team")), _read(silver("fact_team_champion")), _read(gold("team_season"))
    )


@dp.materialized_view(name=gold("skater_validation_history"))
def skater_validation_history():
    return gl.build_skater_history(_read(silver("dim_team")), _read(gold("skater_season")))


# ---- the real 2020-21 forecast ---------------------------------------------------------
@dp.materialized_view(name=gold("team_projection_2021_raw"))
def team_projection_2021_raw():
    return gl.project_champion_asof(
        gl.FORECAST_AS_OF_YEAR,
        dim_team=_read(silver("dim_team")),
        fact_team_champion=_read(silver("fact_team_champion")),
        team_season=_read(gold("team_season")),
    )


@dp.materialized_view(name=gold("championship_odds_2021"))
def championship_odds_2021():
    return gl.build_championship_odds(_read(gold("team_projection_2021_raw")))


@dp.materialized_view(name=gold("best_player_forecast_2021"))
def best_player_forecast_2021():
    return gl.project_top_scorer_asof(
        gl.FORECAST_AS_OF_YEAR, dim_team=_read(silver("dim_team")), skater_season=_read(gold("skater_season"))
    )


@dp.materialized_view(name=gold("actual_2020_21_results"))
def actual_2020_21_results():
    return spark.createDataFrame(gl.ACTUAL_2020_21_ROWS, gl.ACTUAL_2020_21_SCHEMA)  # noqa: F821
