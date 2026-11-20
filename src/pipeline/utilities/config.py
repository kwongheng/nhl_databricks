"""Static configuration for the NHL pipeline (data only, no Spark imports).

Replaces the Fabric ``dbo.config_datasets`` table and the config cell at the top of each
silver notebook. The landing notebook and the pipeline files both import from here.

Note: Unity Catalog stores catalog names in lower case, so ``NHL_data`` and ``nhl_data``
are the same catalog. We use the lower-case form everywhere (including /Volumes paths).
"""
from __future__ import annotations

from dataclasses import dataclass, field

CATALOG = "nhl_data"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

KAGGLE_DATASET = "martinellis/nhl-game-data"
LANDING_ROOT = f"/Volumes/{CATALOG}/landing/files"  # Unity Catalog volume


def bronze(name: str) -> str:
    return f"{BRONZE_SCHEMA}.{name}"


def silver(name: str) -> str:
    return f"{SILVER_SCHEMA}.{name}"


def gold(name: str) -> str:
    return f"{GOLD_SCHEMA}.{name}"


# --------------------------------------------------------------------------------------
# Landing + bronze
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Dataset:
    name: str  # bronze table is nhl_data.bronze.<name>
    source_file: str  # file name inside the Kaggle dataset
    hash_columns: tuple[str, ...]  # columns that define row identity (kept for lineage)

    @property
    def bronze_table(self) -> str:
        return bronze(self.name)

    @property
    def landing_dir(self) -> str:
        # one folder per dataset so Auto Loader watches exactly one directory
        return f"{LANDING_ROOT}/{self.name}"


DATASETS: tuple[Dataset, ...] = (
    Dataset("game", "game.csv", ("game_id", "season", "type")),
    Dataset("team_info", "team_info.csv", ("team_id", "franchiseId", "shortName", "teamName", "abbreviation")),
    Dataset(
        "player_info",
        "player_info.csv",
        ("player_id", "firstName", "lastName", "birthDate", "primaryPosition", "nationality"),
    ),
    Dataset("game_goalie_stats", "game_goalie_stats.csv", ("game_id", "player_id", "team_id", "timeOnIce")),
    Dataset("game_skater_stats", "game_skater_stats.csv", ("game_id", "player_id", "team_id", "timeOnIce")),
    Dataset("game_teams_stats", "game_teams_stats.csv", ("game_id", "team_id", "HoA")),
)


# --------------------------------------------------------------------------------------
# Silver
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class ForeignKey:
    """Rows without a match in ``ref_table`` (a silver table name) are dropped.

    ``keys`` is one column (same name both sides) or (column here, column in ref table).
    """

    ref_table: str
    keys: tuple[str, ...]

    @property
    def ref_column(self) -> str:
        return self.keys[-1]


@dataclass(frozen=True)
class SilverTable:
    name: str  # reads bronze.<name>, writes silver.<name>
    # column -> Spark type. Bronze is all text; these types are applied with TRY_CAST, so
    # values such as 'NA' become NULL instead of failing.
    columns: dict[str, str]
    primary_keys: tuple[str, ...]
    not_null: tuple[str, ...] = ()
    fill_null: dict[str, str] = field(default_factory=dict)
    allowed_values: dict[str, tuple[str, ...]] = field(default_factory=dict)
    checks: dict[str, str] = field(default_factory=dict)  # extra SQL expectations
    foreign_keys: tuple[ForeignKey, ...] = ()

    @property
    def bronze_table(self) -> str:
        return bronze(self.name)

    @property
    def silver_table(self) -> str:
        return silver(self.name)


_TEAM_FK = ForeignKey("team_info", ("team_id",))
_GAME_FK = ForeignKey("game", ("game_id",))
_PLAYER_FK = ForeignKey("player_info", ("player_id",))

# game_id is BIGINT on purpose: ids such as 2022020001 overflow a 32-bit INT.
SILVER_TABLES: tuple[SilverTable, ...] = (
    SilverTable(
        name="team_info",
        columns={
            "team_id": "int",
            "franchiseId": "int",
            "shortName": "string",
            "teamName": "string",
            "abbreviation": "string",
        },
        primary_keys=("team_id",),
        not_null=("team_id", "franchiseId", "shortName", "teamName", "abbreviation"),
    ),
    SilverTable(
        name="player_info",
        columns={
            "player_id": "int",
            "firstName": "string",
            "lastName": "string",
            "birthDate": "date",
            "primaryPosition": "string",
            "nationality": "string",
        },
        primary_keys=("player_id",),
        fill_null={"nationality": "None"},
        not_null=("player_id", "firstName", "lastName", "birthDate", "primaryPosition", "nationality"),
        allowed_values={"primaryPosition": ("LW", "D", "C", "RW", "G")},
    ),
    SilverTable(
        name="game",
        columns={
            "game_id": "bigint",
            "season": "string",
            "type": "string",
            "date_time_GMT": "timestamp",
            "home_team_id": "int",
            "away_team_id": "int",
            "home_goals": "int",
            "away_goals": "int",
            "outcome": "string",
        },
        primary_keys=("game_id",),
        # A season runs from Sept 15 of its start year to Sept 30 of its end year.
        checks={
            "game_date_within_season": (
                "date_time_GMT IS NOT NULL "
                "AND date_time_GMT >= to_timestamp(concat(substring(season, 1, 4), '-09-15 00:00:00')) "
                "AND date_time_GMT <= to_timestamp(concat(substring(season, 5, 4), '-09-30 23:59:59'))"
            )
        },
        foreign_keys=(
            ForeignKey("team_info", ("home_team_id", "team_id")),
            ForeignKey("team_info", ("away_team_id", "team_id")),
        ),
    ),
    SilverTable(
        name="game_teams_stats",
        columns={
            "game_id": "bigint",
            "team_id": "int",
            "HoA": "string",
            "won": "boolean",
            "settled_in": "string",
            "goals": "int",
            "shots": "int",
            "hits": "int",
            "pim": "int",
            "powerPlayOpportunities": "int",
            "powerPlayGoals": "int",
            "faceOffWinPercentage": "double",
            "giveaways": "int",
            "takeaways": "int",
            "blocked": "int",
        },
        primary_keys=("game_id", "team_id"),
        not_null=("game_id", "team_id", "HoA", "won", "settled_in"),
        allowed_values={"HoA": ("home", "away"), "settled_in": ("REG", "tbc", "OT")},
        foreign_keys=(_GAME_FK, _TEAM_FK),
    ),
    SilverTable(
        name="game_skater_stats",
        columns={
            "game_id": "bigint",
            "player_id": "int",
            "team_id": "int",
            "timeOnIce": "int",
            "goals": "int",
            "assists": "int",
            "shots": "int",
            "hits": "int",
            "penaltyMinutes": "int",
            "plusMinus": "int",
            "powerPlayGoals": "int",
            "powerPlayAssists": "int",
            "faceOffWins": "int",
            "faceoffTaken": "int",
            "takeaways": "int",
            "giveaways": "int",
        },
        primary_keys=("game_id", "player_id", "team_id"),
        not_null=(
            "game_id", "player_id", "team_id", "timeOnIce", "goals", "assists", "shots",
            "penaltyMinutes", "plusMinus", "powerPlayGoals", "powerPlayAssists",
            "faceOffWins", "faceoffTaken",
        ),
        foreign_keys=(_GAME_FK, _TEAM_FK, _PLAYER_FK),
    ),
    SilverTable(
        name="game_goalie_stats",
        columns={
            "game_id": "bigint",
            "player_id": "int",
            "team_id": "int",
            "timeOnIce": "int",
            "shots": "int",
            "saves": "int",
            "decision": "string",
        },
        primary_keys=("game_id", "player_id", "team_id"),
        not_null=("game_id", "player_id", "team_id", "timeOnIce", "shots", "saves"),
        foreign_keys=(_GAME_FK, _TEAM_FK, _PLAYER_FK),
    ),
)
