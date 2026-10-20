"""Document ranking and deduplication logic.

Ranks LSEG filings by submission type priority, language, page count, and
recency to select the single best annual/ESG report per organization.
"""

from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F

# ---------------------------------------------------------------------------
# Submission type classification (priority + amendment flag)
# ---------------------------------------------------------------------------

SUBMISSION_TYPE_ROWS = [
    # (FeedName, SubmissionTypeDescription, priority, amendment)
    ("CRIS", "Full Year", 0, 0),
    ("CRIS", "AnnualAmendment", 0, 1),
    ("CRIS", "56-1 Full Year", 1, 0),
    ("CRIS", "10-K", 2, 0),
    ("CRIS", "Audited Annual Report", 3, 0),
    ("CRIS", "Annual Integrated Report", 3, 0),
    ("CRIS", "Prelim-Full Year", 4, 0),
    ("CRIS", "Prelim Amendment-Full Year", 4, 1),
    ("China", "ANNUAL FULL REPORT", 1, 0),
    ("China", "ANNUAL FULL REPORT - ENGLISH", 0, 0),
    ("DART", "Annual Report", 0, 0),
    ("DART", "Audited Annual report", 0, 0),
    ("DART", "Consolidated Audited report", 1, 0),
    ("Edgar", "10-K", 0, 0),
    ("Edgar", "10-K/A", 0, 1),
    ("Edgar", "20-F", 1, 0),
    ("Edgar", "20-F/A", 1, 1),
    ("Edgar", "40-F", 2, 0),
    ("Edgar", "40-F/A", 2, 1),
    ("Edgar", "ANNLRPT", 3, 0),
    ("Edgar", "ANNLRPT/A", 3, 1),
    ("Edgar", "ARS", 4, 0),
    ("Edgar", "ARS/A", 4, 1),
    ("OBI", "Prelim Full Year", 1, 0),
    ("OBI", "Reports Annual", 0, 0),
    ("SEDAR", "Annual report - English", 0, 0),
    ("SEDAR", "Annual report (amended)- English", 0, 1),
    ("SEDAR", "Annual report - French", 0, 0),
    ("SEDAR", "Annual report (amended)- French", 0, 1),
    ("SEDAR", "Annual report - foreign language", 0, 0),
    ("SEDAR", "Annual information form - English", 1, 0),
    ("SEDAR", "Revised annual information form - English", 1, 1),
    ("SEDAR", "Annual information form - French", 1, 0),
    ("SEDAR", "Revised annual information form - French", 1, 1),
    ("SEDAR", "Annual report on Form 10-K - English", 2, 0),
    ("SEDAR", "Revised annual report on Form 10-K - English", 2, 1),
    ("SEDAR", "Annual report on Form 10-K - French", 2, 0),
    ("SEDAR", "Revised annual report on Form 10-K - French", 2, 1),
    ("SEDAR", "MD&A - English", 3, 0),
    ("SEDAR", "MD&A (amended) - English", 3, 1),
    ("SEDAR", "MD&A - French", 3, 0),
    ("SEDAR", "MD&A (amended) - French", 3, 1),
    ("SEDAR", "Audited annual financial statements - English", 4, 0),
    ("SEDAR", "Audited annual financial statements (amended) \u2013 English", 4, 1),
    ("SEDAR", "Audited annual financial statements \u2013 French", 4, 0),
    ("SEDAR", "Audited annual financial statements (amended) \u2013 French", 4, 1),
    ("Tanshin", "Tanshin; Consolidated; Annual; 1st page + full document", 0, 0),
    ("Yuho", "Yuho; Annual", 0, 0),
    ("Yuho", "Yuho; Annual Amendment", 0, 1),
]

# Feed-level priority (lower = preferred)
FEED_PRIORITY_ORDER = ["China", "Yuho", "Tanshin", "DART", "CRIS", "Edgar", "SEDAR", "OBI"]

# Refinitiv language ID → name
LANGUAGE_MAP = [
    (505002, "Afrikaans"), (505004, "Albanian"), (505007, "Arabic"),
    (505008, "Arabic - Algeria"), (505037, "Bulgarian"), (505043, "Chinese"),
    (505049, "Chinese (Simplified)"), (505050, "Chinese (Traditional)"),
    (505052, "Croatian"), (505055, "Czech"), (505056, "Danish"),
    (505058, "Dutch"), (505062, "English"), (505077, "Estonian"),
    (505082, "Finnish"), (505084, "French"), (505096, "German"),
    (505103, "Greek"), (505109, "Hebrew"), (505113, "Hungarian"),
    (505115, "Icelandic"), (505117, "Indonesian"), (505123, "Italian"),
    (505126, "Japanese"), (505139, "Korean"), (505145, "Latvian"),
    (505148, "Lithuanian"), (505150, "Macedonian"), (505165, "Norwegian"),
    (505174, "Polish"), (505176, "Portuguese"), (505182, "Romanian"),
    (505185, "Russian"), (505192, "Serbian (Cyrillic) - Serbia"),
    (505199, "Slovak"), (505201, "Slovenian"), (505205, "Spanish"),
    (505228, "Swedish"), (505239, "Thai"), (505246, "Turkish"),
    (505252, "Ukranian"), (505258, "Vietnamese"), (505274, "Bosnian"),
]


# ---------------------------------------------------------------------------
# Spark reference DataFrames
# ---------------------------------------------------------------------------

def get_submission_type_df(spark: SparkSession) -> DataFrame:
    """Create the submission-type classification reference DataFrame."""
    return spark.createDataFrame(
        SUBMISSION_TYPE_ROWS,
        ["feedname", "submissiontypedescription", "priority", "amendment"],
    )


def get_feed_priority_df(spark: SparkSession) -> DataFrame:
    """Create the feed priority reference DataFrame."""
    return spark.createDataFrame(
        [(i, name) for i, name in enumerate(FEED_PRIORITY_ORDER)],
        ["feed_priority", "feedname"],
    )


def get_language_map_df(spark: SparkSession) -> DataFrame:
    """Create the language ID → name reference DataFrame."""
    return spark.createDataFrame(LANGUAGE_MAP, ["languageid", "language_name"])


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _deduplicate(doc_meta: DataFrame, category_id: int) -> DataFrame:
    """Remove near-duplicate filings (same org/feed/title/date/pages)."""
    return (
        doc_meta.where(F.col("midlevelcategoryid") == category_id)
        .withColumn(
            "similarity_rank",
            F.row_number().over(
                Window.partitionBy(
                    "feedname", "organization_id", "documenttitle",
                    "documentdate", "filingdate", "pagecount", "languageid",
                ).orderBy(
                    F.desc_nulls_last("sizeinbytes"), F.desc("document_id")
                )
            ),
        )
        .where(F.col("similarity_rank") == 1)
        .select("organization_id", "document_id")
    )


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------

def rank_annual_reports(
    doc_meta: DataFrame,
    spark: SparkSession,
) -> DataFrame:
    """Rank annual reports to pick the single best per organization.

    Returns a DataFrame with rank=1 rows only.
    """
    language_map = get_language_map_df(spark)
    submission_types = get_submission_type_df(spark)
    feed_priority = get_feed_priority_df(spark)
    dedup_filter = _deduplicate(doc_meta, category_id=1)

    return (
        doc_meta.join(language_map, "languageid", "left")
        .join(submission_types, ["feedname", "submissiontypedescription"], "left")
        .join(feed_priority, "feedname", "left")
        .withColumn(
            "language_priority",
            F.when(F.col("language_name") == "English", 0).otherwise(1),
        )
        .withColumn("pagebucket", F.floor(F.col("pagecount") / 100))
        .join(dedup_filter, ["organization_id", "document_id"])
        .withColumn(
            "rank",
            F.rank().over(
                Window.partitionBy("organization_id").orderBy(
                    F.asc_nulls_last("priority"),
                    F.asc_nulls_last("amendment"),
                    F.asc("language_priority"),
                    F.desc_nulls_last("pagebucket"),
                    F.desc("documentdate"),
                    F.asc("feed_priority"),
                    F.desc("filingdate"),
                    F.desc("pagecount"),
                    F.desc("document_id"),
                )
            ),
        )
        .where("rank = 1")
    )


def rank_esg_reports(
    doc_meta: DataFrame,
    spark: SparkSession,
) -> DataFrame:
    """Rank ESG reports to pick the single best per organization.

    Returns a DataFrame with rank=1 rows only.
    """
    language_map = get_language_map_df(spark)
    dedup_filter = _deduplicate(doc_meta, category_id=26)

    return (
        doc_meta.join(language_map, "languageid", "left")
        .withColumn(
            "language_priority",
            F.when(F.col("language_name") == "English", 0).otherwise(1),
        )
        .withColumn("pagebucket", F.floor(F.col("pagecount") / 100))
        .join(dedup_filter, ["organization_id", "document_id"])
        .withColumn(
            "rank",
            F.rank().over(
                Window.partitionBy("organization_id").orderBy(
                    F.asc("language_priority"),
                    F.desc_nulls_last("pagebucket"),
                    F.desc("documentdate"),
                    F.desc("pagecount"),
                    F.desc("filingdate"),
                    F.desc("document_id"),
                )
            ),
        )
        .where("rank = 1")
    )
