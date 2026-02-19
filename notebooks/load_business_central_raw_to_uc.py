# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Step 2 - Raw JSON in UC Volume to Unity Catalog Managed Tables
# MAGIC
# MAGIC This notebook reads pending raw JSON extracts written by Step 1 and upserts
# MAGIC them into Unity Catalog managed Delta tables.

# COMMAND ----------
# Standard library imports for config parsing, table naming, and timestamps.
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# Keep logs structured for both interactive runs and scheduled jobs.
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("bc_step2_load")


# COMMAND ----------
@dataclass
class LoadConfig:
    """
    Runtime configuration for Step 2 loading.

    Values come from widgets so this notebook can be reused across environments.
    """

    target_catalog: str
    target_schema: str
    metadata_catalog: str
    metadata_schema: str
    primary_key_map: Dict[str, List[str]]
    source_system_name: str
    run_id_filter: Optional[str]
    max_runs_per_entity: int


def _get_widget_or_default(name: str, default_value: str) -> str:
    """
    Read Databricks widget value with graceful fallback to default.

    Useful when static tooling imports this module outside Databricks runtime.
    """
    try:
        dbutils.widgets.text(name, default_value)
        value = dbutils.widgets.get(name)
        return value.strip() if value is not None else default_value
    except Exception:
        return default_value


def _load_json_config(config_file_path: str) -> Dict[str, Any]:
    """Load pipeline JSON config from local/DBFS path. Returns empty dict if missing."""
    if not config_file_path:
        return {}
    try:
        if config_file_path.startswith("dbfs:/"):
            raw_text = dbutils.fs.head(config_file_path, 1024 * 1024)
            return json.loads(raw_text)
        expanded = os.path.expanduser(config_file_path)
        if os.path.exists(expanded):
            with open(expanded, "r", encoding="utf-8") as handle:
                return json.load(handle)
    except Exception as exc:
        logger.warning("Failed to load config file %s; falling back to widgets. Error=%s", config_file_path, exc)
    return {}


def _cfg_value(file_cfg: Dict[str, Any], section: str, key: str, default: Any) -> Any:
    """Get value with precedence: section -> global -> default."""
    if section in file_cfg and isinstance(file_cfg[section], dict) and key in file_cfg[section]:
        return file_cfg[section][key]
    if "global" in file_cfg and isinstance(file_cfg["global"], dict) and key in file_cfg["global"]:
        return file_cfg["global"][key]
    return default


def _parse_primary_key_map(raw: str) -> Dict[str, List[str]]:
    """
    Parse entity->primary_keys mapping from JSON widget.

    Example:
    {"*":["id"], "salesOrders":["id"]}
    """
    if not raw:
        return {}
    parsed = json.loads(raw)
    out: Dict[str, List[str]] = {}
    for entity, keys in parsed.items():
        if isinstance(keys, list):
            out[str(entity)] = [str(k) for k in keys]
        else:
            out[str(entity)] = [str(keys)]
    return out


def load_config() -> LoadConfig:
    """Build LoadConfig from notebook widgets."""
    config_file_path = _get_widget_or_default(
        "config_file_path",
        "config/pipeline_config.json",
    )
    file_cfg = _load_json_config(config_file_path)

    target_catalog = _get_widget_or_default(
        "target_catalog", str(_cfg_value(file_cfg, "step2_load", "target_catalog", "main"))
    )
    target_schema = _get_widget_or_default(
        "target_schema", str(_cfg_value(file_cfg, "step2_load", "target_schema", "business_central"))
    )
    metadata_catalog = _get_widget_or_default(
        "metadata_catalog", str(_cfg_value(file_cfg, "step2_load", "metadata_catalog", "main"))
    )
    metadata_schema = _get_widget_or_default(
        "metadata_schema", str(_cfg_value(file_cfg, "step2_load", "metadata_schema", "business_central"))
    )
    source_system_name = _get_widget_or_default(
        "source_system_name",
        str(_cfg_value(file_cfg, "step2_load", "source_system_name", "dynamics365_business_central")),
    )
    run_id_filter = _get_widget_or_default(
        "run_id_filter", str(_cfg_value(file_cfg, "step2_load", "run_id_filter", ""))
    ) or None
    max_runs_per_entity = int(
        _get_widget_or_default(
            "max_runs_per_entity", str(_cfg_value(file_cfg, "step2_load", "max_runs_per_entity", 100))
        )
    )
    default_pk_map = _cfg_value(file_cfg, "step2_load", "primary_key_map_json", {"*": ["id"]})
    primary_key_map = _parse_primary_key_map(
        _get_widget_or_default("primary_key_map_json", json.dumps(default_pk_map))
    )

    return LoadConfig(
        target_catalog=target_catalog,
        target_schema=target_schema,
        metadata_catalog=metadata_catalog,
        metadata_schema=metadata_schema,
        primary_key_map=primary_key_map,
        source_system_name=source_system_name,
        run_id_filter=run_id_filter,
        max_runs_per_entity=max_runs_per_entity,
    )


# Load once at notebook start.
cfg = load_config()


# COMMAND ----------
def _sanitize_identifier(name: str) -> str:
    """Convert source entity names into safe table suffixes."""
    lowered = name.lower()
    sanitized = re.sub(r"[^a-z0-9_]", "_", lowered)
    return re.sub(r"_+", "_", sanitized).strip("_")


def _target_table_name(config: LoadConfig, entity: str) -> str:
    """Managed Delta target table name for one entity."""
    return f"{config.target_catalog}.{config.target_schema}.bc_{_sanitize_identifier(entity)}"


def _primary_keys_for_entity(config: LoadConfig, entity: str) -> List[str]:
    """Resolve primary key columns for entity using explicit map or wildcard fallback."""
    if entity in config.primary_key_map:
        return config.primary_key_map[entity]
    if "*" in config.primary_key_map:
        return config.primary_key_map["*"]
    return ["id"]


def _manifest_table(config: LoadConfig) -> str:
    """Manifest table produced by Step 1 and consumed by Step 2."""
    return f"{config.metadata_catalog}.{config.metadata_schema}._bc_raw_manifest"


def _load_watermark_table(config: LoadConfig) -> str:
    """Step 2 load watermark/status history table."""
    return f"{config.metadata_catalog}.{config.metadata_schema}._bc_load_watermarks"


def _to_timestamp_literal(dt: datetime) -> str:
    """Format datetime for Spark SQL TIMESTAMP literal usage."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# COMMAND ----------
def ensure_objects(config: LoadConfig) -> None:
    """
    Ensure target schema and load watermark table exist before processing.
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.target_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.target_catalog}.{config.target_schema}")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.metadata_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.metadata_catalog}.{config.metadata_schema}")

    # Keep Step 2 runnable standalone by ensuring the Step 1 manifest table exists.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_manifest_table(config)} (
            run_id STRING,
            entity_name STRING,
            raw_path STRING,
            rows_extracted BIGINT,
            extracted_max_timestamp TIMESTAMP,
            extract_status STRING,
            load_status STRING,
            error_message STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_load_watermark_table(config)} (
            entity_name STRING,
            last_loaded_run_id STRING,
            last_loaded_timestamp TIMESTAMP,
            status STRING,
            rows_merged BIGINT,
            error_message STRING,
            updated_at TIMESTAMP
        ) USING DELTA
        """
    )


def get_pending_manifest_rows(config: LoadConfig) -> DataFrame:
    """
    Return manifest entries that are ready to be loaded.

    By default this selects extract successes with `load_status = 'PENDING'`.
    Optionally constrained to one `run_id_filter`.
    """
    where_parts = ["extract_status IN ('SUCCESS', 'SUCCESS_NO_DATA')", "load_status = 'PENDING'"]
    if config.run_id_filter:
        where_parts.append(f"run_id = '{config.run_id_filter}'")
    where_clause = " AND ".join(where_parts)

    pending_df = spark.sql(
        f"""
        SELECT run_id, entity_name, raw_path, rows_extracted, extracted_max_timestamp
        FROM {_manifest_table(config)}
        WHERE {where_clause}
        ORDER BY run_id ASC, entity_name ASC
        """
    )

    # Apply true per-entity limiting instead of a global LIMIT.
    window = Window.partitionBy("entity_name").orderBy(F.col("run_id").asc())
    return (
        pending_df.withColumn("_entity_rank", F.row_number().over(window))
        .filter(F.col("_entity_rank") <= F.lit(config.max_runs_per_entity))
        .drop("_entity_rank")
    )


def update_manifest_status(
    config: LoadConfig,
    run_id: str,
    entity: str,
    new_status: str,
    error_message: Optional[str] = None,
) -> None:
    """
    Update load status for one manifest row.

    Uses MERGE to update only the targeted row, which is safer for concurrent
    runs than full-table overwrite patterns.
    """
    # Use MERGE to update only the targeted row and avoid full-table overwrite races.
    updates = spark.createDataFrame(
        [(run_id, entity, new_status, error_message or "", datetime.now(timezone.utc))],
        schema="run_id STRING, entity_name STRING, load_status STRING, error_message STRING, updated_at TIMESTAMP",
    )
    updates.createOrReplaceTempView("_manifest_status_update")

    spark.sql(
        f"""
        MERGE INTO {_manifest_table(config)} AS tgt
        USING _manifest_status_update AS src
        ON tgt.run_id = src.run_id AND tgt.entity_name = src.entity_name
        WHEN MATCHED THEN UPDATE SET
          tgt.load_status = src.load_status,
          tgt.error_message = src.error_message,
          tgt.updated_at = src.updated_at
        """
    )


def insert_load_watermark(
    config: LoadConfig,
    entity: str,
    run_id: str,
    loaded_timestamp: Optional[datetime],
    status: str,
    rows_merged: int,
    error_message: Optional[str] = None,
) -> None:
    """Append one Step 2 load status event row."""
    ts_literal = (
        f"TIMESTAMP '{_to_timestamp_literal(loaded_timestamp)}'" if loaded_timestamp is not None else "NULL"
    )
    err = (error_message or "").replace("'", "''")
    spark.sql(
        f"""
        INSERT INTO {_load_watermark_table(config)}
        SELECT
            '{entity}' AS entity_name,
            '{run_id}' AS last_loaded_run_id,
            {ts_literal} AS last_loaded_timestamp,
            '{status}' AS status,
            {rows_merged} AS rows_merged,
            '{err}' AS error_message,
            current_timestamp() AS updated_at
        """
    )


# COMMAND ----------
def read_raw_json(raw_path: str, config: LoadConfig, entity: str, run_id: str) -> DataFrame:
    """
    Read newline JSON raw files and attach ingestion metadata columns.
    """
    text_df = spark.read.text(raw_path)
    json_rdd = text_df.select("value").rdd.map(lambda r: r["value"])
    raw_df = spark.read.json(json_rdd)

    return (
        raw_df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system", F.lit(config.source_system_name))
        .withColumn("_entity_name", F.lit(entity))
        .withColumn("_raw_run_id", F.lit(run_id))
        .withColumn("_raw_path", F.lit(raw_path))
    )


def deduplicate_batch(df: DataFrame, primary_keys: Sequence[str]) -> DataFrame:
    """
    Deduplicate one batch on provided keys, keeping the latest record per key.

    Preference order:
    1) newest lastModifiedDateTime if present
    2) newest _ingestion_timestamp fallback
    """
    if df.rdd.isEmpty():
        return df
    existing_keys = [k for k in primary_keys if k in df.columns]
    if not existing_keys:
        return df
    ordering_col = "lastModifiedDateTime" if "lastModifiedDateTime" in df.columns else "_ingestion_timestamp"
    window = Window.partitionBy(*[F.col(k) for k in existing_keys]).orderBy(F.col(ordering_col).desc())
    return df.withColumn("_row_rank", F.row_number().over(window)).filter(F.col("_row_rank") == 1).drop("_row_rank")


def write_or_merge_to_delta(df: DataFrame, table_name: str, primary_keys: Sequence[str]) -> int:
    """
    Upsert batch into managed Delta table.

    Behavior:
    - create table on first load
    - append when no key columns are present
    - merge (update + insert) when keys exist
    """
    if df.rdd.isEmpty():
        return 0

    # Allow non-breaking schema evolution between loads.
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
        return int(df.count())

    existing_keys = [k for k in primary_keys if k in df.columns]
    if not existing_keys:
        count = int(df.count())
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
        return count

    dedup_df = deduplicate_batch(df, existing_keys)
    src = "src"
    tgt = "tgt"
    condition = " AND ".join([f"{tgt}.{k} = {src}.{k}" for k in existing_keys])
    cols = dedup_df.columns
    update_set = {c: f"{src}.{c}" for c in cols}
    insert_values = {c: f"{src}.{c}" for c in cols}

    (
        DeltaTable.forName(spark, table_name)
        .alias(tgt)
        .merge(dedup_df.alias(src), condition)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    return int(dedup_df.count())


# COMMAND ----------
def process_pending_row(config: LoadConfig, row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process one manifest row from PENDING -> LOADED.

    Flow:
    1) read raw JSON
    2) merge into target managed table
    3) mark manifest as LOADED
    4) append load watermark event
    """
    run_id = row["run_id"]
    entity = row["entity_name"]
    raw_path = row["raw_path"]
    extracted_max_timestamp = row["extracted_max_timestamp"]

    df = read_raw_json(raw_path, config, entity, run_id)
    table_name = _target_table_name(config, entity)
    rows_merged = write_or_merge_to_delta(df, table_name, _primary_keys_for_entity(config, entity))

    update_manifest_status(config, run_id, entity, "LOADED", None)
    insert_load_watermark(
        config=config,
        entity=entity,
        run_id=run_id,
        loaded_timestamp=extracted_max_timestamp,
        status="LOADED",
        rows_merged=rows_merged,
        error_message=None,
    )

    return {
        "run_id": run_id,
        "entity": entity,
        "raw_path": raw_path,
        "rows_merged": rows_merged,
        "status": "LOADED",
        "table_name": table_name,
    }


def run_load_pipeline(config: LoadConfig) -> List[Dict[str, Any]]:
    """
    Execute Step 2 for all pending manifest entries.

    Continues after row-level failures; raises at end when any failures occurred.
    """
    ensure_objects(config)
    pending_df = get_pending_manifest_rows(config)
    pending_rows = [r.asDict() for r in pending_df.collect()]

    if not pending_rows:
        logger.info("No pending raw files found to load.")
        return []

    results: List[Dict[str, Any]] = []
    for row in pending_rows:
        run_id = row["run_id"]
        entity = row["entity_name"]
        try:
            results.append(process_pending_row(config, row))
        except Exception as exc:
            # Persist failure in both manifest and load watermark history.
            logger.exception("Load failed for run_id=%s entity=%s", run_id, entity)
            update_manifest_status(config, run_id, entity, "FAILED", str(exc)[:4000])
            insert_load_watermark(
                config=config,
                entity=entity,
                run_id=run_id,
                loaded_timestamp=row["extracted_max_timestamp"],
                status="FAILED",
                rows_merged=0,
                error_message=str(exc)[:4000],
            )
            results.append(
                {
                    "run_id": run_id,
                    "entity": entity,
                    "raw_path": row["raw_path"],
                    "rows_merged": 0,
                    "status": "FAILED",
                    "error": str(exc),
                }
            )

    # Show run summary for quick troubleshooting in notebook UI.
    if results:
        display(spark.createDataFrame(results))
    failed = [r for r in results if r["status"] == "FAILED"]
    if failed:
        raise RuntimeError(f"Step 2 completed with failures: {[(r['run_id'], r['entity']) for r in failed]}")
    return results


# COMMAND ----------
# Notebook entrypoint for Step 2 execution.
load_results = run_load_pipeline(cfg)
logger.info("Step 2 complete. Pending entries processed=%s", len(load_results))

