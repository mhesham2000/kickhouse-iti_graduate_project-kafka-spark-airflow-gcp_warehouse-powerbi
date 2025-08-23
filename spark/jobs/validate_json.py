#!/usr/bin/env python3
"""
Reads Kafka topics soccer.* (excluding validated./rejected.*),
parses JSON, validates required fields, computes PK + payload hash,
and writes to:
  validated.soccer.<topic>
  rejected.soccer.<topic>    (incl. parse errors)

Notes:
- Dedup + watermark are DISABLED for debugging. See "DEDUP BLOCK" below.
- Uses two streaming queries total (validated-all, rejected-all).
"""

import os, json, time, threading
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import (
    regexp_extract, from_json, col, lit, to_json, struct,
    to_timestamp, from_unixtime, concat_ws, coalesce,
    current_timestamp, unix_timestamp, sha2, when
)

# ------------------------ Kafka config ------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "34.41.123.167:9095")
KAFKA_OPTS = {
    "kafka.bootstrap.servers": BOOTSTRAP,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="clickpipe" password="clickpipe-secret";',
    "kafka.ssl.truststore.location": "/opt/secrets/kafka.truststore.jks",
    "kafka.ssl.truststore.password": "m3troctyKafka2025",
    "kafka.ssl.truststore.type": "JKS",
    "kafka.ssl.endpoint.identification.algorithm": "HTTPS",
}

# ------------------------ Spark session ------------------------
spark = (
    SparkSession.builder
      .appName("JSON-Validation-Pipeline")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ------------------------ Checkpoint root ------------------------
CHK_ROOT = os.path.expanduser(os.getenv("CHK_ROOT", "/tmp/chk"))
os.makedirs(CHK_ROOT, exist_ok=True)

# ------------------------ Primary keys per topic ------------------------
primary = {
    "broadcast":      ["id","strTimeStamp"],
    "event":          ["idEvent"],
    "team":           ["idTeam"],
    "league":         ["idLeague"],
    "venue":          ["idVenue"],
    "schedule":       ["idEvent","strTimestamp"],
    "live_score":     ["idLiveScore"],
    "live.event.lookup": ["idEvent"],
    "event.stats":    ["idEvent","idStatistic"],
    "event.timeline": ["idTimeline"],
    "event.highlights":["idEvent","strFilename"],
    "event.lineup":   ["idLineup"],
    "player":         ["idPlayer"],
}

def add_pk(df: DataFrame, cols):
    # empty strings for nulls so key stays stable
    return df.withColumn("pk", concat_ws("|", *[coalesce(col(c), lit("")) for c in cols]))

def ensure_timestamps(df: DataFrame):
    # Treat non-positive or absurdly old ingested_at as invalid (floor=2020-01-01).
    valid_src = when(col("ingested_at").cast("double") > 1577836800, col("ingested_at").cast("double"))
    df2 = df.withColumn(
        "ingested_at",
        coalesce(
            valid_src,
            unix_timestamp(col("kafka_ts")).cast("double"),
            unix_timestamp().cast("double")
        )
    )
    return df2.withColumn("evt_ts", to_timestamp(from_unixtime(col("ingested_at"))))

# ------------------------ Schemas ------------------------
topics = {
    "broadcast": StructType([
        StructField("id",             StringType()),
        StructField("idEvent",        StringType()),
        StructField("intDivision",    StringType()),
        StructField("idChannel",      StringType()),
        StructField("strChannel",     StringType()),
        StructField("strCountry",     StringType()),
        StructField("strEventCountry",StringType()),
        StructField("strSport",       StringType()),
        StructField("strEvent",       StringType()),
        StructField("strSeason",      StringType()),
        StructField("dateEvent",      StringType()),
        StructField("strTime",        StringType()),
        StructField("strTimeStamp",   StringType()),
        StructField("strLogo",        StringType()),
        StructField("strEventThumb",  StringType()),
        StructField("strEventPoster", StringType()),
        StructField("strEventBanner", StringType()),
        StructField("strEventSquare", StringType()),
        StructField("ingested_at",    DoubleType()),
    ]),
    "event": StructType([
        StructField("idEvent",               StringType()),
        StructField("idLeague",              StringType()),
        StructField("idHomeTeam",            StringType()),
        StructField("idAwayTeam",            StringType()),
        StructField("idVenue",               StringType()),
        StructField("strVenue",              StringType()),
        StructField("strEvent",              StringType()),
        StructField("strSeason",             StringType()),
        StructField("strCountry",            StringType()),
        StructField("strCity",               StringType()),
        StructField("strSport",              StringType()),
        StructField("strDescriptionEN",      StringType()),
        StructField("strHomeTeam",           StringType()),
        StructField("strAwayTeam",           StringType()),
        StructField("intHomeScore",          StringType()),
        StructField("intAwayScore",          StringType()),
        StructField("intRound",              StringType()),
        StructField("intSpectators",         StringType()),
        StructField("intScore",              StringType()),
        StructField("intScoreVotes",         StringType()),
        StructField("strResult",             StringType()),
        StructField("strGroup",              StringType()),
        StructField("strOfficial",           StringType()),
        StructField("strPoster",             StringType()),
        StructField("strSquare",             StringType()),
        StructField("strThumb",              StringType()),
        StructField("strBanner",             StringType()),
        StructField("strMap",                StringType()),
        StructField("strTweet1",             StringType()),
        StructField("strStatus",             StringType()),
        StructField("dateEvent",             StringType()),
        StructField("strEventTime",          StringType()),
        StructField("strTimestamp",          StringType()),
        StructField("strTime",               StringType()),
        StructField("strHomeTeamBadge",      StringType()),
        StructField("strAwayTeamBadge",      StringType()),
        StructField("strFilename",           StringType()),
        StructField("ingested_at",           DoubleType()),
    ]),
    "team": StructType([
        StructField("idTeam",               StringType()),
        StructField("idESPN",               StringType()),
        StructField("idVenue",              StringType()),
        StructField("idLeague",             StringType()),
        StructField("idLeague2",            StringType()),
        StructField("idLeague3",            StringType()),
        StructField("idLeague4",            StringType()),
        StructField("idLeague5",            StringType()),
        StructField("idLeague6",            StringType()),
        StructField("idLeague7",            StringType()),
        StructField("strLeague",            StringType()),
        StructField("strLeague2",           StringType()),
        StructField("strLeague3",           StringType()),
        StructField("strLeague4",           StringType()),
        StructField("strLeague5",           StringType()),
        StructField("strLeague6",           StringType()),
        StructField("strLeague7",           StringType()),
        StructField("strDivision",          StringType()),
        StructField("intFormedYear",        StringType()),
        StructField("strTeam",              StringType()),
        StructField("strTeamAlternate",     StringType()),
        StructField("strTeamShort",         StringType()),
        StructField("strSport",             StringType()),
        StructField("strStadium",           StringType()),
        StructField("intStadiumCapacity",   StringType()),
        StructField("strLocation",          StringType()),
        StructField("strCountry",           StringType()),
        StructField("strKeywords",          StringType()),
        StructField("strRSS",               StringType()),
        StructField("strDescriptionEN",     StringType()),
        StructField("strColour1",           StringType()),
        StructField("strColour2",           StringType()),
        StructField("strColour3",           StringType()),
        StructField("strEquipment",         StringType()),
        StructField("strGender",            StringType()),
        StructField("strWebsite",           StringType()),
        StructField("strBadge",             StringType()),
        StructField("strLogo",              StringType()),
        StructField("strFanart1",           StringType()),
        StructField("strFanart2",           StringType()),
        StructField("strFanart3",           StringType()),
        StructField("strFanart4",           StringType()),
        StructField("strBanner",            StringType()),
        StructField("ingested_at",          DoubleType()),
    ]),
        "live.event.lookup": StructType([
        StructField("idEvent",              StringType()),
        StructField("idAPIfootball",        StringType()),
        StructField("strEvent",             StringType()),
        StructField("strEventAlternate",    StringType()),
        StructField("strFilename",          StringType()),
        StructField("strSport",             StringType()),
        StructField("idLeague",             StringType()),
        StructField("strLeague",            StringType()),
        StructField("strLeagueBadge",       StringType()),
        StructField("strSeason",            StringType()),
        StructField("strDescriptionEN",     StringType()),
        StructField("strHomeTeam",          StringType()),
        StructField("strAwayTeam",          StringType()),
        StructField("intHomeScore",         StringType()),
        StructField("intRound",             StringType()),
        StructField("intAwayScore",         StringType()),
        StructField("intSpectators",        StringType()),
        StructField("strOfficial",          StringType()),
        StructField("strTimestamp",         StringType()),
        StructField("dateEvent",            StringType()),
        StructField("dateEventLocal",       StringType()),
        StructField("strTime",              StringType()),
        StructField("strTimeLocal",         StringType()),
        StructField("strGroup",             StringType()),
        StructField("idHomeTeam",           StringType()),
        StructField("strHomeTeamBadge",     StringType()),
        StructField("idAwayTeam",           StringType()),
        StructField("strAwayTeamBadge",     StringType()),
        StructField("intScore",             StringType()),
        StructField("intScoreVotes",        StringType()),
        StructField("strResult",            StringType()),
        StructField("idVenue",              StringType()),
        StructField("strVenue",             StringType()),
        StructField("strCountry",           StringType()),
        StructField("strCity",              StringType()),
        StructField("strPoster",            StringType()),
        StructField("strSquare",            StringType()),
        StructField("strFanart",            StringType()),
        StructField("strThumb",             StringType()),
        StructField("strBanner",            StringType()),
        StructField("strMap",               StringType()),
        StructField("strTweet1",            StringType()),
        StructField("strTweet2",            StringType()),
        StructField("strTweet3",            StringType()),
        StructField("strVideo",             StringType()),
        StructField("strStatus",            StringType()),
        StructField("strPostponed",         StringType()),
        StructField("strLocked",            StringType()),
        StructField("ingested_at",          DoubleType()),
    ]),
    "league": StructType([
        StructField("idLeague",             StringType()),
        StructField("idAPIfootball",        StringType()),
        StructField("idSoccerXML",          StringType()),
        StructField("idCup",                StringType()),
        StructField("intDivision",          StringType()),
        StructField("strCurrentSeason",     StringType()),
        StructField("intFormedYear",        StringType()),
        StructField("strGender",            StringType()),
        StructField("strTvRights",          StringType()),
        StructField("strLeague",            StringType()),
        StructField("strSport",             StringType()),
        StructField("strLeagueAlternate",   StringType()),
        StructField("strCountry",           StringType()),
        StructField("strDescriptionEN",     StringType()),
        StructField("strFanart1",           StringType()),
        StructField("strFanart2",           StringType()),
        StructField("strFanart3",           StringType()),
        StructField("strBanner",            StringType()),
        StructField("strBadge",             StringType()),
        StructField("strLogo",              StringType()),
        StructField("strPoster",            StringType()),
        StructField("strTrophy",            StringType()),
        StructField("strNaming",            StringType()),
        StructField("strWebsite",           StringType()),
        StructField("strFacebook",          StringType()),
        StructField("strRSS",               StringType()),
        StructField("ingested_at",          DoubleType()),
    ]),
    "venue": StructType([
        StructField("idVenue",            StringType()),
        StructField("idDupe",             StringType()),
        StructField("intFormedYear",      StringType()),
        StructField("strCost",            StringType()),
        StructField("strArchitect",       StringType()),
        StructField("strVenueSponsor",    StringType()),
        StructField("strVenueAlternate",  StringType()),
        StructField("strVenue",           StringType()),
        StructField("strSport",           StringType()),
        StructField("strLocation",        StringType()),
        StructField("strCountry",         StringType()),
        StructField("intCapacity",        StringType()),
        StructField("strDescriptionEN",   StringType()),
        StructField("strFanart1",         StringType()),
        StructField("strFanart2",         StringType()),
        StructField("strFanart3",         StringType()),
        StructField("strThumb",           StringType()),
        StructField("strLogo",            StringType()),
        StructField("strMap",             StringType()),
        StructField("strWebsite",         StringType()),
        StructField("strCreativeCommons", StringType()),
        StructField("strTimezone",        StringType()),
        StructField("ingested_at",        DoubleType()),
    ]),
    "schedule": StructType([
        StructField("idEvent",          StringType()),
        StructField("idHomeTeam",       StringType()),
        StructField("idAwayTeam",       StringType()),
        StructField("strEvent",         StringType()),
        StructField("strSport",         StringType()),
        StructField("strHomeTeam",      StringType()),
        StructField("strAwayTeam",      StringType()),
        StructField("intHomeScore",     StringType()),
        StructField("intAwayScore",     StringType()),
        StructField("strStatus",        StringType()),
        StructField("strCountry",       StringType()),
        StructField("strVenue",         StringType()),
        StructField("strThumb",         StringType()),
        StructField("strHomeTeamBadge", StringType()),
        StructField("strAwayTeamBadge", StringType()),
        StructField("strTimeLocal",     StringType()),
        StructField("strTime",          StringType()),
        StructField("strTimestamp",     StringType()),
        StructField("dateEvent",        StringType()),
        StructField("ingested_at",      DoubleType()),
    ]),
    "live_score": StructType([
        StructField("idLiveScore",          StringType()),
        StructField("idEvent",              StringType()),
        StructField("idLeague",             StringType()),
        StructField("idHomeTeam",           StringType()),
        StructField("idAwayTeam",           StringType()),
        StructField("intHomeScore",         StringType()),
        StructField("intAwayScore",         StringType()),
        StructField("strLeague",            StringType()),
        StructField("strHomeTeam",          StringType()),
        StructField("strAwayTeam",          StringType()),
        StructField("strStatus",            StringType()),
        StructField("strSport",             StringType()),
        StructField("strEventTime",         StringType()),
        StructField("dateEvent",            StringType()),
        StructField("strHomeTeamBadge",     StringType()),
        StructField("strAwayTeamBadge",     StringType()),
        StructField("intEventScore",        StringType()),
        StructField("intEventScoreTotal",   StringType()),
        StructField("strProgress",          StringType()),
        StructField("updated",              StringType()),
        StructField("ingested_at",          DoubleType()),
    ]),
    "event.stats": StructType([
        StructField("idEvent",       StringType()),
        StructField("idStatistic",   StringType()),
        StructField("idApiFootball", StringType()),
        StructField("strEvent",      StringType()),
        StructField("strStat",       StringType()),
        StructField("intHome",       StringType()),
        StructField("intAway",       StringType()),
        StructField("ingested_at",   DoubleType()),
    ]),
    "event.timeline": StructType([
        StructField("idTimeline",        StringType()),
        StructField("idEvent",           StringType()),
        StructField("strTimeline",       StringType()),
        StructField("strTimelineDetail", StringType()),
        StructField("strHome",           StringType()),
        StructField("strEvent",          StringType()),
        StructField("idAPIfootball",     StringType()),
        StructField("idPlayer",          StringType()),
        StructField("strPlayer",         StringType()),
        StructField("strCountry",        StringType()),
        StructField("idAssist",          StringType()),
        StructField("strAssist",         StringType()),
        StructField("intTime",           StringType()),
        StructField("idTeam",            StringType()),
        StructField("strTeam",           StringType()),
        StructField("strComment",        StringType()),
        StructField("dateEvent",         StringType()),
        StructField("strSeason",         StringType()),
        StructField("ingested_at",       DoubleType()),
    ]),
    "event.highlights": StructType([
        StructField("idEvent",           StringType()),
        StructField("idAPIfootball",     StringType()),
        StructField("idHomeTeam",        StringType()),
        StructField("idAwayTeam",        StringType()),
        StructField("idVenue",           StringType()),
        StructField("strEvent",          StringType()),
        StructField("strEventAlternate", StringType()),
        StructField("strFilename",       StringType()),
        StructField("strSport",          StringType()),
        StructField("idLeague",          StringType()),
        StructField("strLeague",         StringType()),
        StructField("strLeagueBadge",    StringType()),
        StructField("strSeason",         StringType()),
        StructField("strDescriptionEN",  StringType()),
        StructField("strHomeTeam",       StringType()),
        StructField("strAwayTeam",       StringType()),
        StructField("intHomeScore",      StringType()),
        StructField("intAwayScore",      StringType()),
        StructField("intRound",          StringType()),
        StructField("intScore",          StringType()),
        StructField("intScoreVotes",     StringType()),
        StructField("strResult",         StringType()),
        StructField("strOfficial",       StringType()),
        StructField("strGroup",          StringType()),
        StructField("strHomeTeamBadge",  StringType()),
        StructField("strAwayTeamBadge",  StringType()),
        StructField("strCountry",        StringType()),
        StructField("strPoster",         StringType()),
        StructField("strSquare",         StringType()),
        StructField("strFanart",         StringType()),
        StructField("strThumb",          StringType()),
        StructField("strBanner",         StringType()),
        StructField("strMap",            StringType()),
        StructField("strTweet1",         StringType()),
        StructField("strTweet2",         StringType()),
        StructField("strTweet3",         StringType()),
        StructField("strVideo",          StringType()),
        StructField("dateEvent",         StringType()),
        StructField("strTime",           StringType()),
        StructField("strStatus",         StringType()),
        StructField("strPostponed",      StringType()),
        StructField("strLocked",         StringType()),
        StructField("ingested_at",       DoubleType()),
    ]),
    "event.lineup": StructType([
        StructField("idLineup",         StringType()),
        StructField("idEvent",          StringType()),
        StructField("strEvent",         StringType()),
        StructField("strPosition",      StringType()),
        StructField("strPositionShort", StringType()),
        StructField("strHome",          StringType()),
        StructField("strSubstitute",    StringType()),
        StructField("intSquadNumber",   StringType()),
        StructField("strCutout",        StringType()),
        StructField("idPlayer",         StringType()),
        StructField("strPlayer",        StringType()),
        StructField("idTeam",           StringType()),
        StructField("strTeam",          StringType()),
        StructField("strSeason",        StringType()),
        StructField("strCountry",       StringType()),
        StructField("ingested_at",      DoubleType()),
    ]),
    "player": StructType([
        StructField("idPlayer",      StringType()),
        StructField("idTeam",        StringType()),
        StructField("lookup_player", StructType([
            StructField("idTeam2",              StringType()),
            StructField("idTeamNational",       StringType()),
            StructField("idAPIfootball",        StringType()),
            StructField("idPlayerManager",      StringType()),
            StructField("idWikidata",           StringType()),
            StructField("idTransferMkt",        StringType()),
            StructField("idESPN",               StringType()),
            StructField("intSoccerXMLTeamID",   StringType()),
            StructField("strNationality",       StringType()),
            StructField("strPlayer",            StringType()),
            StructField("strPlayerAlternate",   StringType()),
            StructField("strSport",             StringType()),
            StructField("dateBorn",             StringType()),
            StructField("dateDied",             StringType()),
            StructField("dateSigned",           StringType()),
            StructField("strSigning",           StringType()),
            StructField("strWage",              StringType()),
            StructField("strOutfitter",         StringType()),
            StructField("strKit",               StringType()),
            StructField("strAgent",             StringType()),
            StructField("strBirthLocation",     StringType()),
            StructField("strEthnicity",         StringType()),
            StructField("strGender",            StringType()),
            StructField("strSide",              StringType()),
            StructField("strCollege",           StringType()),
            StructField("strFacebook",          StringType()),
            StructField("strWebsite",           StringType()),
            StructField("strTwitter",           StringType()),
            StructField("strInstagram",         StringType()),
            StructField("strYoutube",           StringType()),
            StructField("strThumb",             StringType()),
            StructField("strPoster",            StringType()),
            StructField("strCutout",            StringType()),
            StructField("strRender",            StringType()),
            StructField("strBanner",            StringType()),
            StructField("strFanart1",           StringType()),
            StructField("strFanart2",           StringType()),
            StructField("strFanart3",           StringType()),
            StructField("strFanart4",           StringType()),
            StructField("strCreativeCommons",   StringType()),
            StructField("strNumber",            StringType()),
            StructField("strPosition",          StringType()),
            StructField("strStatus",            StringType()),
            StructField("strHeight",            StringType()),
            StructField("strWeight",            StringType()),
            StructField("strTeam",              StringType()),
            StructField("strTeam2",             StringType()),
            StructField("strDescriptionEN",     StringType())
        ])),
        StructField("ingested_at",   DoubleType()),
    ]),
}

# ------------------------ Required fields ------------------------
required = {
    "broadcast":            ["id", "idEvent", "strSport", "ingested_at"],
    "event":                ["idEvent", "ingested_at"],  # partial allowed
    "team":                 ["idTeam", "strTeam", "strSport", "idLeague", "ingested_at"],
    "league":               ["idLeague", "strLeague", "strSport", "strCountry", "ingested_at"],
    "venue":                ["idVenue", "strVenue", "strSport", "strLocation", "strCountry", "ingested_at"],
    "schedule":             ["idEvent", "strEvent", "strSport", "idHomeTeam", "idAwayTeam",
                            "strTimestamp", "dateEvent", "strStatus", "strCountry", "ingested_at"],
    "live_score":           ["idLiveScore", "idEvent", "strSport", "idLeague", "idHomeTeam",
                            "idAwayTeam", "intHomeScore", "intAwayScore", "strStatus",
                            "strEventTime", "dateEvent", "updated", "ingested_at"],
    "live.event.lookup":    ["idEvent", "strSport", "idLeague", "idHomeTeam", "idAwayTeam",
                            "strStatus", "strTimestamp", "dateEvent", "ingested_at"],  # <-- NEW
    "event.stats":          ["idEvent", "idStatistic", "strStat", "intHome", "intAway", "ingested_at"],
    "event.timeline":       ["idTimeline", "idEvent", "strTimeline", "idPlayer", "ingested_at"],
    "event.highlights":     ["idEvent", "idAPIfootball", "strEvent", "intHomeScore", "intAwayScore", "ingested_at"],
    "event.lineup":         ["idLineup", "idEvent", "strPosition", "idPlayer", "ingested_at"],
    "player":               ["idPlayer", "idTeam", "lookup_player", "ingested_at"]
}

# Which column holds the sport per topic (nested path allowed)
SPORT_FIELD_MAP = {
    "broadcast":          "strSport",
    "event":              "strSport",
    "team":               "strSport",
    "league":             "strSport",
    "venue":              "strSport",
    "schedule":           "strSport",
    "live_score":         "strSport",
    "live.event.lookup":  "strSport",
    "event.highlights":   "strSport",
    "player":             "lookup_player.strSport",  # nested
    # NOTE: event.stats, event.timeline, event.lineup do not carry a sport field
}

EXCLUDE_FROM_HASH = {
    "ingested_at", "evt_ts", "pk", "is_valid",
    "kafka_ts", "json_str", "parse_error",
    "sport_ok"  # <-- add this
}


# ------------------------ Reader ------------------------
raw = (
  spark.readStream.format("kafka")
    .options(**KAFKA_OPTS)
    .option("subscribePattern", r"^(?!validated\.|rejected\.)soccer\..*")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

topic_name_col = regexp_extract(col("topic"), r'^soccer\.(.+)$', 1)

def validate(df: DataFrame, name: str) -> DataFrame:
    # Base required-fields check (always boolean, never NULL)
    must_have = reduce(lambda a, b: a & b, [col(c).isNotNull() for c in required[name]])
    out = df.withColumn("is_valid", must_have)

    # Apply soccer filter where the topic actually carries a sport field
    sport_field = SPORT_FIELD_MAP.get(name)
    if sport_field:
        # Resolve nested path safely
        sport_col = col(sport_field)
        # Force a deterministic boolean (NULL -> False) to avoid tri-state surprises
        out = (out.withColumn("sport_ok", when(sport_col.rlike("(?i)soccer"), lit(True)).otherwise(lit(False)))
                  .withColumn("is_valid", col("is_valid") & col("sport_ok")))
    return out


def add_payload_hash(df: DataFrame) -> DataFrame:
    cols_for_hash = [c for c in df.columns if c not in EXCLUDE_FROM_HASH]
    ordered = sorted(cols_for_hash)
    if ordered:
        payload_col = to_json(struct(*[col(c) for c in ordered]))
    else:
        payload_col = lit("")  # fallback
    return df.withColumn("_payload_json", payload_col) \
             .withColumn("payload_hash", sha2(col("_payload_json"), 256)) \
             .drop("_payload_json")

# ------------------------ Build per-topic flows, union into 2 streams ------------------------
validated_frames = []
rejected_frames  = []

for name, schema in topics.items():
    base = (raw
        .filter(topic_name_col == name)
        .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_ts"),
            col("value").cast("string").alias("json_str")
        ))

    # Parse OK path
    parsed_ok  = base.filter(col("data").isNotNull()).select("data.*", "kafka_ts", "json_str")
    stamped    = ensure_timestamps(parsed_ok)
    keyed      = add_pk(stamped, primary[name])
    validated  = validate(keyed, name)
    hashed     = add_payload_hash(validated)

    # --------- DEDUP BLOCK (DISABLED) ----------
    # To enable later, replace 'dedup = hashed' with the 2 lines below.
    # dedup = (hashed
    #          .withWatermark("evt_ts","48 hours")
    #          .dropDuplicates(["pk","payload_hash"]))
    dedup = hashed
    # -------------------------------------------

    cols_out = [c for c in dedup.columns if c not in ("pk","evt_ts","payload_hash","is_valid")]

    validated_frames.append(
        dedup.filter("is_valid")
             .withColumn("topic", lit(f"validated.soccer.{name}"))
             .withColumn("key", col("pk").cast("string"))
             .withColumn("value", to_json(struct(*[col(c) for c in cols_out])))
             .select("topic","key","value")
    )

    rejected_frames.append(
        dedup.filter("NOT is_valid")
             .withColumn("topic", lit(f"rejected.soccer.{name}"))
             .withColumn("key", col("pk").cast("string"))
             .withColumn("value", to_json(struct(*[col(c) for c in cols_out])))
             .select("topic","key","value")
    )

    # Parse-bad path (payload isn't valid JSON for this schema)
    parse_bad = (base.filter(col("data").isNull())
        .select("kafka_ts", "json_str")
        .withColumn("pk", sha2(col("json_str"), 256))
        .withColumn("ingested_at", unix_timestamp(col("kafka_ts")).cast("double"))
        .withColumn("evt_ts", to_timestamp(col("kafka_ts")))
        .withColumn("parse_error", lit(True)))

    parse_bad_h = add_payload_hash(parse_bad)

    parse_cols_out = [c for c in parse_bad_h.columns if c not in ("pk","evt_ts","payload_hash")]
    rejected_frames.append(
        parse_bad_h
            .withColumn("topic", lit(f"rejected.soccer.{name}"))
            .withColumn("key", col("pk").cast("string"))
            .withColumn("value", to_json(struct(*[col(c) for c in parse_cols_out])))
            .select("topic","key","value")
    )

def _union_all(frames):
    if not frames:
        return spark.createDataFrame([], schema=StructType([
            StructField("topic", StringType()),
            StructField("key",   StringType()),
            StructField("value", StringType()),
        ]))
    return reduce(lambda a,b: a.unionByName(b, allowMissingColumns=True), frames)

validated_all = _union_all(validated_frames)
rejected_all  = _union_all(rejected_frames)

# ------------------------ Optional debug sinks ------------------------
if os.getenv("DEBUG_CONSOLE", "0") == "1":
    (validated_all.writeStream
        .format("console").option("truncate","false").option("numRows", 20)
        .option("checkpointLocation", os.path.join(CHK_ROOT, "console-validated"))
        .start())
    (rejected_all.writeStream
        .format("console").option("truncate","false").option("numRows", 20)
        .option("checkpointLocation", os.path.join(CHK_ROOT, "console-rejected"))
        .start())

# ------------------------ Kafka sinks (only two queries) ------------------------
def kafka_sink(df: DataFrame, chk_name: str):
    return (df
        .selectExpr("CAST(topic AS STRING) AS topic",
                    "CAST(key   AS STRING) AS key",
                    "CAST(value AS STRING) AS value")
        .writeStream
        .format("kafka")
        .options(**KAFKA_OPTS)
        # topic comes from column "topic"
        .option("checkpointLocation", os.path.join(CHK_ROOT, chk_name))
        .option("kafka.enable.idempotence", "true")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start())

q_valid = kafka_sink(validated_all, "validated-all")
q_rej   = kafka_sink(rejected_all,  "rejected-all")

# ------------------------ Optional progress logger ------------------------
def progress_logger():
    import json, time
    while True:
        try:
            for q in spark.streams.active:
                msg = q.status.get("message", "")
                lp = q.lastProgress
                print(f"[{time.strftime('%H:%M:%S')}] {q.name or 'query'} :: {msg}")
                if lp: print(json.dumps(lp, indent=2))
        except Exception as e:
            print("progress_logger error:", e)
        time.sleep(10)

if os.getenv("DEBUG_PROGRESS", "0") == "1":
    threading.Thread(target=progress_logger, daemon=True).start()

spark.streams.awaitAnyTermination()
