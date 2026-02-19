# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Step 0 - Discover Business Central Entities and Build Selection List
# MAGIC
# MAGIC This notebook discovers available Business Central entities and helps you
# MAGIC build the `entities` JSON list used by Step 1.

# COMMAND ----------
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from pyspark.sql import functions as F

# COMMAND ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("bc_step0_discover")


# COMMAND ----------
@dataclass
class DiscoveryConfig:
    tenant_id: str
    environment: str
    company_id: str
    client_id: str
    client_secret: str
    oauth_scope: str
    api_version: str
    metadata_catalog: str
    metadata_schema: str
    include_pattern: Optional[str]
    exclude_pattern: Optional[str]
    selected_entities_csv: Optional[str]
    max_retries: int
    retry_backoff_seconds: float
    request_timeout_seconds: int


def _get_widget_or_default(name: str, default_value: str) -> str:
    try:
        dbutils.widgets.text(name, default_value)
        value = dbutils.widgets.get(name)
        return value.strip() if value is not None else default_value
    except Exception:
        return default_value


def _parse_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


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


def load_config() -> DiscoveryConfig:
    config_file_path = _get_widget_or_default("config_file_path", "config/pipeline_config.json")
    file_cfg = _load_json_config(config_file_path)

    tenant_id = _get_widget_or_default("bc_tenant_id", str(_cfg_value(file_cfg, "step0_discovery", "bc_tenant_id", "")))
    environment = _get_widget_or_default(
        "bc_environment", str(_cfg_value(file_cfg, "step0_discovery", "bc_environment", "production"))
    )
    company_id = _get_widget_or_default(
        "bc_company_id", str(_cfg_value(file_cfg, "step0_discovery", "bc_company_id", ""))
    )
    client_id = _get_widget_or_default("bc_client_id", str(_cfg_value(file_cfg, "step0_discovery", "bc_client_id", "")))
    client_secret = _get_widget_or_default(
        "bc_client_secret", str(_cfg_value(file_cfg, "step0_discovery", "bc_client_secret", ""))
    )
    oauth_scope = _get_widget_or_default(
        "bc_oauth_scope",
        str(
            _cfg_value(
                file_cfg,
                "step0_discovery",
                "bc_oauth_scope",
                "https://api.businesscentral.dynamics.com/.default",
            )
        ),
    )
    api_version = _get_widget_or_default(
        "bc_api_version", str(_cfg_value(file_cfg, "step0_discovery", "bc_api_version", "v2.0"))
    )

    metadata_catalog = _get_widget_or_default(
        "metadata_catalog", str(_cfg_value(file_cfg, "step0_discovery", "metadata_catalog", "main"))
    )
    metadata_schema = _get_widget_or_default(
        "metadata_schema", str(_cfg_value(file_cfg, "step0_discovery", "metadata_schema", "business_central"))
    )

    include_pattern = _get_widget_or_default(
        "include_pattern", str(_cfg_value(file_cfg, "step0_discovery", "include_pattern", ""))
    ) or None
    exclude_pattern = _get_widget_or_default(
        "exclude_pattern", str(_cfg_value(file_cfg, "step0_discovery", "exclude_pattern", ""))
    ) or None
    selected_entities_csv = _get_widget_or_default(
        "selected_entities_csv", str(_cfg_value(file_cfg, "step0_discovery", "selected_entities_csv", ""))
    ) or None

    max_retries = int(
        _get_widget_or_default("max_retries", str(_cfg_value(file_cfg, "step0_discovery", "max_retries", 5)))
    )
    retry_backoff_seconds = float(
        _get_widget_or_default(
            "retry_backoff_seconds", str(_cfg_value(file_cfg, "step0_discovery", "retry_backoff_seconds", 2))
        )
    )
    request_timeout_seconds = int(
        _get_widget_or_default(
            "request_timeout_seconds", str(_cfg_value(file_cfg, "step0_discovery", "request_timeout_seconds", 60))
        )
    )

    required = {
        "bc_tenant_id": tenant_id,
        "bc_company_id": company_id,
        "bc_client_id": client_id,
        "bc_client_secret": client_secret,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required widgets: {', '.join(missing)}")

    return DiscoveryConfig(
        tenant_id=tenant_id,
        environment=environment,
        company_id=company_id,
        client_id=client_id,
        client_secret=client_secret,
        oauth_scope=oauth_scope,
        api_version=api_version,
        metadata_catalog=metadata_catalog,
        metadata_schema=metadata_schema,
        include_pattern=include_pattern,
        exclude_pattern=exclude_pattern,
        selected_entities_csv=selected_entities_csv,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        request_timeout_seconds=request_timeout_seconds,
    )


cfg = load_config()


# COMMAND ----------
class BusinessCentralAuthClient:
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, scope: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self._access_token: Optional[str] = None
        self._expires_at_utc: Optional[datetime] = None
        self._token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    def get_access_token(self) -> str:
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
        self._expires_at_utc = datetime.now(timezone.utc) + timedelta(seconds=max(expires_in - 120, 60))
        return self._access_token


class BusinessCentralDiscoveryClient:
    def __init__(self, config: DiscoveryConfig, auth_client: BusinessCentralAuthClient):
        self.cfg = config
        self.auth_client = auth_client
        self.session = requests.Session()

    @property
    def base_url(self) -> str:
        return (
            f"https://api.businesscentral.dynamics.com/v2.0/"
            f"{self.cfg.tenant_id}/{self.cfg.environment}/api/{self.cfg.api_version}/"
            f"companies({self.cfg.company_id})"
        )

    def _request(self, url: str) -> Dict[str, Any]:
        for attempt in range(1, self.cfg.max_retries + 1):
            headers = {
                "Authorization": f"Bearer {self.auth_client.get_access_token()}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            try:
                response = self.session.get(
                    url=url,
                    headers=headers,
                    timeout=self.cfg.request_timeout_seconds,
                )
            except requests.RequestException as exc:
                wait_seconds = self.cfg.retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning("Network error during discovery; retrying in %.1fs. %s", wait_seconds, exc)
                time.sleep(wait_seconds)
                continue

            if response.status_code in (429, 500, 502, 503, 504):
                wait_seconds = self.cfg.retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning("Retryable status %s; retrying in %.1fs", response.status_code, wait_seconds)
                time.sleep(wait_seconds)
                continue

            if response.status_code >= 400:
                raise RuntimeError(
                    f"Discovery API error {response.status_code}: {response.text}"
                )

            return response.json()

        raise RuntimeError("Discovery request failed after retries.")

    def discover_entity_names(self) -> List[str]:
        # OData service document lists available entity collections under `value`.
        payload = self._request(self.base_url)
        values = payload.get("value", [])
        names = [v.get("name") for v in values if isinstance(v, dict) and v.get("name")]
        unique_sorted = sorted(set(str(x) for x in names))
        return unique_sorted


# COMMAND ----------
def _entity_catalog_table(config: DiscoveryConfig) -> str:
    return f"{config.metadata_catalog}.{config.metadata_schema}._bc_entity_catalog"


def ensure_metadata_objects(config: DiscoveryConfig) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.metadata_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.metadata_catalog}.{config.metadata_schema}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_entity_catalog_table(config)} (
            discovered_at TIMESTAMP,
            entity_name STRING,
            include_pattern STRING,
            exclude_pattern STRING,
            selected_for_ingestion BOOLEAN
        ) USING DELTA
        """
    )


def apply_filters(entities: List[str], include_pattern: Optional[str], exclude_pattern: Optional[str]) -> List[str]:
    filtered = entities
    if include_pattern:
        include_re = re.compile(include_pattern)
        filtered = [e for e in filtered if include_re.search(e)]
    if exclude_pattern:
        exclude_re = re.compile(exclude_pattern)
        filtered = [e for e in filtered if not exclude_re.search(e)]
    return filtered


def build_selection(entities: List[str], selected_csv: Optional[str]) -> List[str]:
    # If user provides explicit CSV selection, use that; otherwise use all discovered entities.
    explicit = _parse_csv(selected_csv)
    if explicit:
        chosen = [e for e in explicit if e in entities]
        missing = [e for e in explicit if e not in entities]
        if missing:
            logger.warning("Ignoring selected entities not discovered: %s", missing)
        return sorted(set(chosen))
    return entities


def persist_entity_catalog(
    config: DiscoveryConfig,
    discovered_entities: List[str],
    selected_entities: List[str],
) -> None:
    selected_set = set(selected_entities)
    rows = [
        (
            datetime.now(timezone.utc),
            e,
            config.include_pattern or "",
            config.exclude_pattern or "",
            e in selected_set,
        )
        for e in discovered_entities
    ]

    df = spark.createDataFrame(
        rows,
        schema="""
            discovered_at TIMESTAMP,
            entity_name STRING,
            include_pattern STRING,
            exclude_pattern STRING,
            selected_for_ingestion BOOLEAN
        """,
    )
    df.write.mode("append").saveAsTable(_entity_catalog_table(config))


# COMMAND ----------
ensure_metadata_objects(cfg)

auth_client = BusinessCentralAuthClient(
    tenant_id=cfg.tenant_id,
    client_id=cfg.client_id,
    client_secret=cfg.client_secret,
    scope=cfg.oauth_scope,
)
discovery_client = BusinessCentralDiscoveryClient(cfg, auth_client)

all_entities = discovery_client.discover_entity_names()
filtered_entities = apply_filters(all_entities, cfg.include_pattern, cfg.exclude_pattern)
selected_entities = build_selection(filtered_entities, cfg.selected_entities_csv)

persist_entity_catalog(cfg, filtered_entities, selected_entities)

summary_rows = [
    ("total_discovered", len(all_entities)),
    ("after_filters", len(filtered_entities)),
    ("selected_for_ingestion", len(selected_entities)),
]
display(spark.createDataFrame(summary_rows, "metric STRING, value INT"))

entities_json = json.dumps(selected_entities)
print("Use this value for Step 1 widget `entities`:")
print(entities_json)

display(
    spark.createDataFrame([(e,) for e in selected_entities], "entity_name STRING")
    .withColumn("selected_for_ingestion", F.lit(True))
)

# Useful in workflows: this notebook can return the ready-to-use JSON string.
dbutils.notebook.exit(entities_json)

