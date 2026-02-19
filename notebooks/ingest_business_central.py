# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Step 1 - Business Central to Raw JSON in UC Volume (Incremental)
# MAGIC
# MAGIC This notebook extracts incremental data from Dynamics 365 Business Central
# MAGIC and writes raw JSON files to a Unity Catalog Volume.

# COMMAND ----------
# Standard library imports for config parsing, timestamps, retries, and typing.
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from pyspark.sql import functions as F

# COMMAND ----------
# Keep logs structured so notebook output is usable in both interactive runs and jobs.
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("bc_step1_extract")


# COMMAND ----------
@dataclass
class ExtractConfig:
    """
    Runtime configuration for Step 1 extraction.

    All values are sourced from Databricks widgets so the same notebook can run
    across environments without code changes.
    """

    tenant_id: str
    environment: str
    company_id: str
    client_id: str
    client_secret: str
    oauth_scope: str
    api_version: str
    raw_volume_path: str
    metadata_catalog: str
    metadata_schema: str
    entities: List[str]
    page_size: int
    request_timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    initial_load_start_utc: Optional[str]


def _get_widget_or_default(name: str, default_value: str) -> str:
    """
    Read a Databricks widget safely.

    If widgets are unavailable (for example, local linting/static analysis), the
    provided default is returned.
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


def _parse_entities(raw_entities: Any) -> List[str]:
    """
    Parse entities from either JSON list format or comma-separated format.

    Supported inputs:
    - '["customers","items"]'
    - 'customers,items'
    """
    if raw_entities is None:
        return []
    if isinstance(raw_entities, list):
        return [str(x).strip() for x in raw_entities if str(x).strip()]
    raw_text = str(raw_entities)
    if not raw_text:
        return []
    stripped = raw_text.strip()
    if stripped.startswith("["):
        parsed = json.loads(stripped)
        return [str(x).strip() for x in parsed if str(x).strip()]
    return [x.strip() for x in stripped.split(",") if x.strip()]


def load_config() -> ExtractConfig:
    """
    Build and validate ExtractConfig from notebook widgets.

    Required BC credentials are validated here so failures happen early before
    any network calls or table writes.
    """
    config_file_path = _get_widget_or_default(
        "config_file_path",
        "config/pipeline_config.json",
    )
    file_cfg = _load_json_config(config_file_path)

    tenant_id = _get_widget_or_default("bc_tenant_id", str(_cfg_value(file_cfg, "step1_ingest", "bc_tenant_id", "")))
    environment = _get_widget_or_default(
        "bc_environment", str(_cfg_value(file_cfg, "step1_ingest", "bc_environment", "production"))
    )
    company_id = _get_widget_or_default(
        "bc_company_id", str(_cfg_value(file_cfg, "step1_ingest", "bc_company_id", ""))
    )
    client_id = _get_widget_or_default("bc_client_id", str(_cfg_value(file_cfg, "step1_ingest", "bc_client_id", "")))
    client_secret = _get_widget_or_default(
        "bc_client_secret", str(_cfg_value(file_cfg, "step1_ingest", "bc_client_secret", ""))
    )
    oauth_scope = _get_widget_or_default(
        "bc_oauth_scope",
        str(
            _cfg_value(
                file_cfg,
                "step1_ingest",
                "bc_oauth_scope",
                "https://api.businesscentral.dynamics.com/.default",
            )
        ),
    )
    api_version = _get_widget_or_default(
        "bc_api_version", str(_cfg_value(file_cfg, "step1_ingest", "bc_api_version", "v2.0"))
    )
    raw_volume_path = _get_widget_or_default(
        "raw_volume_path",
        str(_cfg_value(file_cfg, "step1_ingest", "raw_volume_path", "/Volumes/main/business_central/bc_raw/raw")),
    )
    metadata_catalog = _get_widget_or_default(
        "metadata_catalog", str(_cfg_value(file_cfg, "step1_ingest", "metadata_catalog", "main"))
    )
    metadata_schema = _get_widget_or_default(
        "metadata_schema", str(_cfg_value(file_cfg, "step1_ingest", "metadata_schema", "business_central"))
    )

    default_entities = _cfg_value(file_cfg, "step1_ingest", "entities", ["customers", "items", "salesOrders"])
    entities = _parse_entities(_get_widget_or_default("entities", json.dumps(default_entities)))
    page_size = int(_get_widget_or_default("page_size", str(_cfg_value(file_cfg, "step1_ingest", "page_size", 1000))))
    request_timeout_seconds = int(
        _get_widget_or_default(
            "request_timeout_seconds", str(_cfg_value(file_cfg, "step1_ingest", "request_timeout_seconds", 60))
        )
    )
    max_retries = int(_get_widget_or_default("max_retries", str(_cfg_value(file_cfg, "step1_ingest", "max_retries", 5))))
    retry_backoff_seconds = float(
        _get_widget_or_default(
            "retry_backoff_seconds", str(_cfg_value(file_cfg, "step1_ingest", "retry_backoff_seconds", 2))
        )
    )
    initial_load_start_utc = _get_widget_or_default(
        "initial_load_start_utc", str(_cfg_value(file_cfg, "step1_ingest", "initial_load_start_utc", ""))
    ) or None

    required = {
        "bc_tenant_id": tenant_id,
        "bc_company_id": company_id,
        "bc_client_id": client_id,
        "bc_client_secret": client_secret,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required widgets: {', '.join(missing)}")
    if not entities:
        raise ValueError("No entities configured.")

    return ExtractConfig(
        tenant_id=tenant_id,
        environment=environment,
        company_id=company_id,
        client_id=client_id,
        client_secret=client_secret,
        oauth_scope=oauth_scope,
        api_version=api_version,
        raw_volume_path=raw_volume_path,
        metadata_catalog=metadata_catalog,
        metadata_schema=metadata_schema,
        entities=entities,
        page_size=page_size,
        request_timeout_seconds=request_timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        initial_load_start_utc=initial_load_start_utc,
    )


# Load configuration once at notebook start and log the entities to process.
cfg = load_config()
logger.info("Configured entities: %s", cfg.entities)


# COMMAND ----------
class BusinessCentralAuthClient:
    """Small auth client that caches and refreshes OAuth access tokens."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str, scope: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self._access_token: Optional[str] = None
        self._expires_at_utc: Optional[datetime] = None
        self._token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    def get_access_token(self) -> str:
        """
        Return a valid bearer token.

        Reuses a cached token until shortly before expiration to reduce auth
        round-trips and avoid unnecessary pressure on Microsoft identity APIs.
        """
        now = datetime.now(timezone.utc)
        if self._access_token and self._expires_at_utc and now < self._expires_at_utc:
            return self._access_token
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": self.scope,
        }
        response = requests.post(self._token_url, data=payload, timeout=30)
        if response.status_code >= 400:
            raise RuntimeError(f"OAuth failure ({response.status_code}): {response.text}")
        token_json = response.json()
        self._access_token = token_json["access_token"]
        expires_in = int(token_json.get("expires_in", 3600))
        # Refresh two minutes early as a safety margin for long requests.
        self._expires_at_utc = datetime.now(timezone.utc) + timedelta(seconds=max(expires_in - 120, 60))
        return self._access_token


class BusinessCentralApiClient:
    """Business Central API client with retry/backoff and pagination support."""

    def __init__(self, config: ExtractConfig, auth_client: BusinessCentralAuthClient):
        self.cfg = config
        self.auth_client = auth_client
        self.session = requests.Session()

    @property
    def base_entity_url(self) -> str:
        """Base URL prefix for all entity endpoints in the selected BC environment."""
        return (
            f"https://api.businesscentral.dynamics.com/v2.0/"
            f"{self.cfg.tenant_id}/{self.cfg.environment}/api/{self.cfg.api_version}/"
            f"companies({self.cfg.company_id})"
        )

    def _request(self, url: str, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Execute one GET request with retry policy for transient failures.

        Retries:
        - network exceptions
        - HTTP 429 and 5xx responses
        """
        last_error: Optional[Exception] = None
        for attempt in range(1, self.cfg.max_retries + 1):
            headers = {
                "Authorization": f"Bearer {self.auth_client.get_access_token()}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            try:
                response = self.session.get(
                    url=url,
                    params=params,
                    headers=headers,
                    timeout=self.cfg.request_timeout_seconds,
                )
            except requests.RequestException as exc:
                last_error = exc
                wait_seconds = self.cfg.retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning("Network error for %s; retrying in %.1fs", url, wait_seconds)
                time.sleep(wait_seconds)
                continue

            if response.status_code in (429, 500, 502, 503, 504):
                wait_seconds = self.cfg.retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Retryable status %s for %s; retrying in %.1fs",
                    response.status_code,
                    url,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            if response.status_code >= 400:
                raise RuntimeError(
                    f"Business Central API error {response.status_code} for {url}: {response.text}"
                )
            return response.json()

        raise RuntimeError(f"Request failed after retries for {url}. Last error={last_error}")

    def fetch_incremental(
        self, entity: str, watermark_utc: Optional[datetime]
    ) -> Tuple[List[Dict[str, Any]], Optional[datetime]]:
        """
        Fetch one entity incrementally using lastModifiedDateTime watermark.

        Returns:
        - all extracted rows for the entity
        - max lastModifiedDateTime observed in this extraction window
        """
        url = f"{self.base_entity_url}/{entity}"
        params: Dict[str, Any] = {"$top": self.cfg.page_size, "$orderby": "lastModifiedDateTime asc"}

        if watermark_utc:
            wm_text = watermark_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            params["$filter"] = f"lastModifiedDateTime gt {wm_text}"

        rows: List[Dict[str, Any]] = []
        max_last_modified = watermark_utc

        while True:
            # Fetch one page and append rows.
            payload = self._request(url, params)
            page_rows = payload.get("value", [])
            rows.extend(page_rows)

            # Track the highest modified timestamp seen in this batch.
            for row in page_rows:
                parsed = _parse_bc_datetime(row.get("lastModifiedDateTime"))
                if parsed and (max_last_modified is None or parsed > max_last_modified):
                    max_last_modified = parsed

            # OData pagination: follow @odata.nextLink until exhausted.
            next_link = payload.get("@odata.nextLink")
            if not next_link:
                break
            url = next_link
            params = None

        return rows, max_last_modified


# COMMAND ----------
def _parse_bc_datetime(raw: Optional[str]) -> Optional[datetime]:
    """Parse BC ISO datetime values and normalize to UTC."""
    if not raw:
        return None
    cleaned = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_timestamp_literal(dt: datetime) -> str:
    """Format datetime for Spark SQL TIMESTAMP literal usage."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _sanitize_identifier(name: str) -> str:
    """Convert source names to safe table/path fragments."""
    lowered = name.lower()
    sanitized = re.sub(r"[^a-z0-9_]", "_", lowered)
    return re.sub(r"_+", "_", sanitized).strip("_")


def _volume_root(raw_volume_path: str) -> Tuple[str, str, str]:
    """
    Parse `/Volumes/<catalog>/<schema>/<volume>/...` into UC root components.

    This is used to create the volume if needed.
    """
    parts = raw_volume_path.strip("/").split("/")
    if len(parts) < 4 or parts[0] != "Volumes":
        raise ValueError(
            "raw_volume_path must be /Volumes/<catalog>/<schema>/<volume>/optional/subpath"
        )
    return parts[1], parts[2], parts[3]


def _raw_entity_run_path(config: ExtractConfig, entity: str, run_id: str) -> str:
    """Physical raw file path partitioned by entity and run_id."""
    return f"{config.raw_volume_path.rstrip('/')}/entity={_sanitize_identifier(entity)}/run_id={run_id}"


def _extract_watermark_table(config: ExtractConfig) -> str:
    """Metadata table storing latest successful extraction timestamp per entity."""
    return f"{config.metadata_catalog}.{config.metadata_schema}._bc_extract_watermarks"


def _manifest_table(config: ExtractConfig) -> str:
    """Manifest table used as handoff contract between Step 1 and Step 2."""
    return f"{config.metadata_catalog}.{config.metadata_schema}._bc_raw_manifest"


# COMMAND ----------
def ensure_objects(config: ExtractConfig) -> None:
    """
    Create required metadata objects and volume paths if they do not exist.

    Objects created:
    - extraction watermark table
    - raw manifest table
    - UC volume path for raw JSON
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.metadata_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.metadata_catalog}.{config.metadata_schema}")

    vol_catalog, vol_schema, vol_name = _volume_root(config.raw_volume_path)
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {vol_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {vol_catalog}.{vol_schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {vol_catalog}.{vol_schema}.{vol_name}")
    # Ensure the sub-directory under the volume exists.
    dbutils.fs.mkdirs(config.raw_volume_path)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_extract_watermark_table(config)} (
            entity_name STRING,
            last_extracted_timestamp TIMESTAMP,
            last_run_id STRING,
            status STRING,
            error_message STRING,
            updated_at TIMESTAMP
        ) USING DELTA
        """
    )

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


def get_extract_watermark(config: ExtractConfig, entity: str) -> Optional[datetime]:
    """
    Get latest extraction watermark for an entity.

    If no watermark exists yet, optionally falls back to `initial_load_start_utc`.
    """
    rows = spark.sql(
        f"""
        SELECT last_extracted_timestamp
        FROM {_extract_watermark_table(config)}
        WHERE entity_name = '{entity}'
        ORDER BY updated_at DESC
        LIMIT 1
        """
    ).collect()

    if not rows or rows[0]["last_extracted_timestamp"] is None:
        return _parse_bc_datetime(config.initial_load_start_utc) if config.initial_load_start_utc else None

    ts = rows[0]["last_extracted_timestamp"]
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def insert_extract_watermark(
    config: ExtractConfig,
    entity: str,
    last_timestamp: Optional[datetime],
    run_id: str,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """Append one extraction watermark event row."""
    ts_literal = (
        f"TIMESTAMP '{_to_timestamp_literal(last_timestamp)}'" if last_timestamp is not None else "NULL"
    )
    err = (error_message or "").replace("'", "''")
    spark.sql(
        f"""
        INSERT INTO {_extract_watermark_table(config)}
        SELECT
            '{entity}' AS entity_name,
            {ts_literal} AS last_extracted_timestamp,
            '{run_id}' AS last_run_id,
            '{status}' AS status,
            '{err}' AS error_message,
            current_timestamp() AS updated_at
        """
    )


def insert_manifest_row(
    config: ExtractConfig,
    run_id: str,
    entity: str,
    raw_path: str,
    rows_extracted: int,
    extracted_max_timestamp: Optional[datetime],
    extract_status: str,
    load_status: str,
    error_message: Optional[str] = None,
) -> None:
    """
    Append one manifest row for Step 2 consumption.

    `extract_status` describes extraction outcome.
    `load_status` is initialized for Step 2 state transitions.
    """
    ts_literal = (
        f"TIMESTAMP '{_to_timestamp_literal(extracted_max_timestamp)}'"
        if extracted_max_timestamp is not None
        else "NULL"
    )
    err = (error_message or "").replace("'", "''")
    spark.sql(
        f"""
        INSERT INTO {_manifest_table(config)}
        SELECT
            '{run_id}' AS run_id,
            '{entity}' AS entity_name,
            '{raw_path}' AS raw_path,
            {rows_extracted} AS rows_extracted,
            {ts_literal} AS extracted_max_timestamp,
            '{extract_status}' AS extract_status,
            '{load_status}' AS load_status,
            '{err}' AS error_message,
            current_timestamp() AS created_at,
            current_timestamp() AS updated_at
        """
    )


def write_raw_json(config: ExtractConfig, entity: str, run_id: str, rows: List[Dict[str, Any]]) -> str:
    """
    Persist extracted records as newline JSON text files in the raw volume.

    Returns the raw path even if there are zero rows (for consistent manifesting).
    """
    raw_path = _raw_entity_run_path(config, entity, run_id)
    if rows:
        json_rows = [json.dumps(r, separators=(",", ":")) for r in rows]
        spark.createDataFrame(json_rows, "string").toDF("value").write.mode("overwrite").text(raw_path)
    return raw_path


# COMMAND ----------
def process_entity(config: ExtractConfig, bc_client: BusinessCentralApiClient, entity: str, run_id: str) -> Dict[str, Any]:
    """
    End-to-end Step 1 logic for one entity.

    Flow:
    1) read watermark
    2) fetch incremental rows
    3) write raw JSON
    4) write manifest + extraction watermark
    """
    watermark = get_extract_watermark(config, entity)
    logger.info("Entity=%s watermark=%s", entity, watermark)

    rows, max_modified = bc_client.fetch_incremental(entity, watermark)
    rows_extracted = len(rows)

    raw_path = write_raw_json(config, entity, run_id, rows)
    status = "SUCCESS_NO_DATA" if rows_extracted == 0 else "SUCCESS"

    insert_manifest_row(
        config=config,
        run_id=run_id,
        entity=entity,
        raw_path=raw_path,
        rows_extracted=rows_extracted,
        extracted_max_timestamp=max_modified or watermark,
        extract_status=status,
        load_status="PENDING" if rows_extracted > 0 else "SKIPPED_NO_DATA",
        error_message=None,
    )
    insert_extract_watermark(
        config=config,
        entity=entity,
        last_timestamp=max_modified or watermark,
        run_id=run_id,
        status=status,
        error_message=None,
    )

    return {
        "run_id": run_id,
        "entity": entity,
        "rows_extracted": rows_extracted,
        "raw_path": raw_path,
        "extract_status": status,
        "next_step_load_status": "PENDING" if rows_extracted > 0 else "SKIPPED_NO_DATA",
    }


def run_extract_pipeline(config: ExtractConfig) -> List[Dict[str, Any]]:
    """
    Execute Step 1 for all configured entities.

    The run continues entity-by-entity even when one fails, then raises at the
    end if failures occurred so orchestrators can alert.
    """
    ensure_objects(config)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    logger.info("Starting extract run_id=%s", run_id)

    auth_client = BusinessCentralAuthClient(
        tenant_id=config.tenant_id,
        client_id=config.client_id,
        client_secret=config.client_secret,
        scope=config.oauth_scope,
    )
    bc_client = BusinessCentralApiClient(config, auth_client)

    results: List[Dict[str, Any]] = []
    for entity in config.entities:
        try:
            results.append(process_entity(config, bc_client, entity, run_id))
        except Exception as exc:
            # Capture failed entity in both manifest and watermark history.
            logger.exception("Extract failed for entity=%s", entity)
            raw_path = _raw_entity_run_path(config, entity, run_id)
            insert_manifest_row(
                config=config,
                run_id=run_id,
                entity=entity,
                raw_path=raw_path,
                rows_extracted=0,
                extracted_max_timestamp=get_extract_watermark(config, entity),
                extract_status="FAILED",
                load_status="BLOCKED",
                error_message=str(exc)[:4000],
            )
            insert_extract_watermark(
                config=config,
                entity=entity,
                last_timestamp=get_extract_watermark(config, entity),
                run_id=run_id,
                status="FAILED",
                error_message=str(exc)[:4000],
            )
            results.append(
                {
                    "run_id": run_id,
                    "entity": entity,
                    "rows_extracted": 0,
                    "raw_path": raw_path,
                    "extract_status": "FAILED",
                    "next_step_load_status": "BLOCKED",
                    "error": str(exc),
                }
            )

    # Show a compact run summary in notebook output.
    display(spark.createDataFrame(results))
    failed = [r for r in results if r["extract_status"] == "FAILED"]
    if failed:
        raise RuntimeError(f"Step 1 completed with failures: {[r['entity'] for r in failed]}")
    return results


# COMMAND ----------
# Notebook entrypoint for Step 1 execution.
extract_results = run_extract_pipeline(cfg)
logger.info("Step 1 complete. Entities processed=%s", len(extract_results))

