import gc
import json
import logging
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ChunkedEncodingError, ConnectionError, HTTPError
from urllib3.util.retry import Retry
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, array_except, col, current_date, lit, regexp_replace
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

MAX_WORKERS = 6
SEMAPHORE_LIMIT = 8
REQUEST_DELAY_MIN = 0.1
REQUEST_DELAY_MAX = 0.2
NUM_BATCHES = 8
INCLUIR_REGISTROS_TABELA = False
MAIN_TABLE = "tech.bronze.purview_columns_assets"
CUSTOM_METADATA_TABLE = "tech.bronze.purview_custom_metadata"
ASSETS_SEM_COLUNAS_TABLE = "tech.bronze.assets_sem_colunas"
SALVAR_ASSETS_SEM_COLUNAS = True

_assets_sem_colunas_lock = threading.Lock()
_assets_sem_colunas_global = []


def get_purview_account_name() -> str:
    return os.getenv("endpointxp", "endpointxp")


def get_purview_endpoint() -> str:
    return f"endpointxp"


def get_location() -> str:
    return "abfss://bronze@corpprdlaketechst.dfs.core.windows.net/tabela"


def get_location_custom_attributes_table() -> str:
    return "abfss://bronze@corpprdlaketechst.dfs.core.windows.net/tabela"


def get_location_assets_sem_colunas_table() -> str:
    return "abfss://bronze@corpprdlaketechst.dfs.core.windows.net/tabela"


def get_location_log_table() -> str:
    return "abfss://bronze@corpprdlaketechst.dfs.core.windows.net/tabela"


def get_snapshot_date():
    now = datetime.now()
    return now.date() + timedelta(days=1) if now.hour >= 20 else now.date()


def get_credentials():
    #removido para segurança


def get_access_token(tenant_id, client_id, client_secret):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    response = requests.post(
        url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://purview.azure.net/.default",
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def get_token():
    return get_access_token(*get_credentials())


def extract_column_tags(labels):
    if not labels:
        return []
    if isinstance(labels, dict):
        return list(labels.keys())
    if isinstance(labels, list):
        return labels
    return [str(labels)]


def remove_display_text_from_array(column_tag, display_text):
    if column_tag is None:
        return None
    return [value for value in column_tag if value != display_text]


def get_all_assets(
    access_token,
    purview_endpoint,
    max_retries=5,
    backoff_factor=1.0,
    status_forcelist=(500, 502, 503, 504),
):
    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    url = f"{purview_endpoint}/datamap/api/search/query?api-version=2023-09-01"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    all_assets = []
    continuation_token = None

    while True:
        payload = {"keywords": "*", "limit": 1000}
        if continuation_token:
            payload["continuationToken"] = continuation_token

        for attempt in range(1, max_retries + 1):
            try:
                response = session.post(url, headers=headers, json=payload, timeout=60)
                response.raise_for_status()
                data = response.json()
                break
            except (ChunkedEncodingError, ConnectionError) as exc:
                wait = backoff_factor * (2 ** (attempt - 1))
                logger.warning(
                    "Falha de conexão na consulta de assets. Tentativa %s/%s; nova tentativa em %.1fs",
                    attempt,
                    max_retries,
                    wait,
                )
                time.sleep(wait)
                if attempt == max_retries:
                    raise RuntimeError("Limite de tentativas atingido") from exc
            except HTTPError:
                logger.exception("Erro HTTP ao consultar assets: status=%s", response.status_code)
                raise

        all_assets.extend(data.get("value", []))
        continuation_token = data.get("continuationToken")
        if not continuation_token:
            break

    return all_assets


def get_table_type_names():
    return ["databricks_unity_catalog_table", "databricks_table"]


def extract_schema_from_qualified_name(qualified_name):
    if not qualified_name:
        return None
    try:
        if "/schemas/" in qualified_name and "/tables/" in qualified_name:
            schemas_pos = qualified_name.find("/schemas/")
            tables_pos = qualified_name.find("/tables/")
            if schemas_pos != -1 and tables_pos > schemas_pos:
                return qualified_name[schemas_pos + len("/schemas/") : tables_pos]
        return None
    except Exception:
        logger.exception("Erro ao extrair schema de qualifiedName")
        return None


def filter_databricks_assets(all_assets, schemas_filter=None):
    table_assets = [asset for asset in all_assets if asset.get("objectType") == "Tables"]
    databricks_assets = []

    for asset in table_assets:
        qualified_name = asset.get("qualifiedName", "").lower()
        entity_type = asset.get("entityType", "").lower()
        is_databricks = (
            "databricks" in qualified_name
            or "unity" in qualified_name
            or entity_type in get_table_type_names()
            or qualified_name.startswith("databricks://")
        )
        if not is_databricks:
            continue

        schema_name = extract_schema_from_qualified_name(qualified_name)
        if schema_name:
            if schemas_filter is None or schema_name in schemas_filter:
                databricks_assets.append(asset)
        elif schemas_filter is None:
            databricks_assets.append(asset)

    if not databricks_assets:
        logger.warning("Nenhuma tabela Databricks identificada; usando todos os assets do tipo Tables")
        return table_assets

    logger.info("Tabelas Databricks selecionadas: %s", len(databricks_assets))
    return databricks_assets


def get_all_assets_filtered(
    access_token,
    purview_endpoint,
    schemas_filter=None,
    max_retries=5,
    backoff_factor=1.0,
    status_forcelist=(500, 502, 503, 504),
):
    all_assets = get_all_assets(
        access_token, purview_endpoint, max_retries, backoff_factor, status_forcelist
    )
    return filter_databricks_assets(all_assets, schemas_filter)


def adicionar_asset_sem_colunas(asset_data):
    if SALVAR_ASSETS_SEM_COLUNAS:
        with _assets_sem_colunas_lock:
            _assets_sem_colunas_global.append(asset_data)


def obter_e_limpar_assets_sem_colunas():
    with _assets_sem_colunas_lock:
        assets = _assets_sem_colunas_global.copy()
        _assets_sem_colunas_global.clear()
        return assets


def get_entity_details_sync(entity_guid: str) -> dict:
    try:
        session = requests.Session()
        retry_strategy = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        url = f"{get_purview_endpoint()}/catalog/api/atlas/v2/entity/guid/{entity_guid}"
        response = session.get(
            url,
            headers={"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"},
            timeout=30,
        )
        if response.status_code == 200:
            return response.json()
        logger.error("Falha ao buscar entidade %s: HTTP %s", entity_guid, response.status_code)
        return None
    except Exception:
        logger.exception("Erro ao buscar entidade %s", entity_guid)
        return None


def process_asset_sync(asset):
    try:
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
        details = get_entity_details_sync(asset["id"])
        if details is None:
            return []

        referred = details.get("referredEntities", {})
        table_business_attributes = details.get("entity", {}).get("businessAttributes", {})

        if not referred:
            adicionar_asset_sem_colunas(
                {
                    **asset,
                    "motivo_sem_colunas": "referredEntities vazio - tabela sem colunas identificadas",
                    "table_business_attributes": table_business_attributes,
                    "detalhes_debug": {
                        "tem_business_attributes": bool(table_business_attributes),
                        "total_business_attributes": len(table_business_attributes)
                        if table_business_attributes
                        else 0,
                    },
                }
            )
            if table_business_attributes:
                return [
                    {
                        **asset,
                        "column_guid": None,
                        "column_name": None,
                        "column_classifications": [],
                        "column_type": None,
                        "businessAttributes": table_business_attributes,
                        "column_tag": [],
                        "column_description": None,
                        "_skip_main_table": True,
                    }
                ]
            return []

        results = []
        for column_guid, column_entity in referred.items():
            if not column_entity.get("typeName", "").endswith("_column"):
                continue
            column_attributes = column_entity.get("attributes", {})
            column_classifications = [
                classification.get("typeName", "")
                for classification in column_entity.get("classifications", [])
            ]
            results.append(
                {
                    **asset,
                    "column_guid": column_guid,
                    "column_name": column_attributes.get("name"),
                    "column_classifications": column_classifications,
                    "column_type": column_attributes.get("type"),
                    "businessAttributes": column_entity.get("businessAttributes", {}) or None,
                    "column_tag": extract_column_tags(column_attributes.get("labels", [])),
                    "column_description": column_attributes.get("description"),
                }
            )

        if table_business_attributes:
            results.append(
                {
                    **asset,
                    "column_guid": None,
                    "column_name": None,
                    "column_classifications": [],
                    "column_type": None,
                    "businessAttributes": table_business_attributes,
                    "column_tag": [],
                    "column_description": None,
                    "_skip_main_table": True,
                }
            )
        return results
    except Exception:
        logger.exception("Erro ao processar asset %s", asset.get("id", "unknown"))
        return []


def process_assets_batch_optimized(assets_batch, batch_num, total_batches, max_workers=MAX_WORKERS):
    semaphore = threading.Semaphore(SEMAPHORE_LIMIT)
    batch_results = []

    def process_single_asset(asset):
        with semaphore:
            return process_asset_sync(asset)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_asset = {
            executor.submit(process_single_asset, asset): asset for asset in assets_batch
        }
        for future in as_completed(future_to_asset):
            try:
                batch_results.extend(future.result())
            except Exception:
                logger.exception(
                    "Erro no asset %s", future_to_asset[future].get("id", "unknown")
                )

    logger.info(
        "Lote %s/%s concluído: %s registros de %s assets",
        batch_num,
        total_batches,
        len(batch_results),
        len(assets_batch),
    )
    return batch_results


def _main_schema():
    return StructType(
        [
            StructField("@search.score", DoubleType(), True),
            StructField("assetType", ArrayType(StringType()), True),
            StructField("classification", StringType(), True),
            StructField("collectionId", StringType(), True),
            StructField("column_classifications", ArrayType(StringType()), True),
            StructField("column_guid", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("column_type", StringType(), True),
            StructField("column_description", StringType(), True),
            StructField("createBy", StringType(), True),
            StructField("createTime", LongType(), True),
            StructField("description", StringType(), True),
            StructField("displayText", StringType(), True),
            StructField("domainId", StringType(), True),
            StructField("entityType", StringType(), True),
            StructField("id", StringType(), True),
            StructField("isIndexed", BooleanType(), True),
            StructField("name", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("qualifiedName", StringType(), True),
            StructField("updateBy", StringType(), True),
            StructField("updateTime", LongType(), True),
            StructField(
                "businessAttributes",
                MapType(StringType(), MapType(StringType(), StringType())),
                True,
            ),
            StructField("column_tag", ArrayType(StringType()), True),
            StructField(
                "contact",
                ArrayType(
                    StructType(
                        [
                            StructField("contactType", StringType(), True),
                            StructField("id", StringType(), True),
                            StructField("info", StringType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )


def save_batch_data_optimized(results, spark, batch_num):
    if not results:
        return 0, 0

    main_table_results = [record for record in results if not record.get("_skip_main_table", False)]
    for record in main_table_results:
        record.pop("_skip_main_table", None)

    main_count = 0
    if main_table_results:
        dataframe = (
            spark.createDataFrame(main_table_results, schema=_main_schema())
            .withColumn("data_snapshot", lit(get_snapshot_date()).cast("date"))
            .withColumn("column_description", regexp_replace(col("column_description"), "<[^>]*>", ""))
            .withColumn("column_tag", array_except(col("column_tag"), array(col("displayText"))))
            .coalesce(4)
        )
        dataframe.write.format("delta").mode("append").saveAsTable(MAIN_TABLE)
        main_count = len(main_table_results)
        time.sleep(2)

    custom_metadata_results = [
        {key: value for key, value in record.items() if key != "_skip_main_table"}
        for record in results
    ]
    custom_count = save_custom_metadata_batch(custom_metadata_results, spark, batch_num)
    logger.info(
        "Lote %s salvo: principal=%s, custom_metadata=%s", batch_num, main_count, custom_count
    )
    gc.collect()
    return main_count, custom_count


def save_assets_sem_colunas(assets_sem_colunas, spark):
    if not assets_sem_colunas or not SALVAR_ASSETS_SEM_COLUNAS:
        return 0

    schema = StructType(
        [
            StructField("@search.score", DoubleType(), True),
            StructField("assetType", ArrayType(StringType()), True),
            StructField("classification", StringType(), True),
            StructField("collectionId", StringType(), True),
            StructField("createBy", StringType(), True),
            StructField("createTime", LongType(), True),
            StructField("description", StringType(), True),
            StructField("displayText", StringType(), True),
            StructField("domainId", StringType(), True),
            StructField("entityType", StringType(), True),
            StructField("id", StringType(), True),
            StructField("isIndexed", BooleanType(), True),
            StructField("name", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("qualifiedName", StringType(), True),
            StructField("updateBy", StringType(), True),
            StructField("updateTime", LongType(), True),
            StructField("motivo_sem_colunas", StringType(), True),
            StructField("tem_business_attributes", BooleanType(), True),
            StructField("total_business_attributes", LongType(), True),
            StructField("schema_identificado", StringType(), True),
            StructField(
                "contact",
                ArrayType(
                    StructType(
                        [
                            StructField("contactType", StringType(), True),
                            StructField("id", StringType(), True),
                            StructField("info", StringType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )

    prepared = []
    for asset in assets_sem_colunas:
        asset_copy = asset.copy()
        details = asset_copy.pop("detalhes_debug", {})
        asset_copy.pop("table_business_attributes", {})
        asset_copy["tem_business_attributes"] = details.get("tem_business_attributes", False)
        asset_copy["total_business_attributes"] = details.get("total_business_attributes", 0)
        asset_copy["schema_identificado"] = extract_schema_from_qualified_name(
            asset.get("qualifiedName", "")
        )
        prepared.append(asset_copy)

    dataframe = spark.createDataFrame(prepared, schema=schema).withColumn(
        "data_snapshot", lit(get_snapshot_date())
    )
    dataframe.persist()
    try:
        (
            dataframe.write.mode("append")
            .partitionBy("data_snapshot")
            .option("path", get_location_assets_sem_colunas_table())
            .saveAsTable(ASSETS_SEM_COLUNAS_TABLE)
        )
        logger.info("Assets sem colunas salvos: %s", len(prepared))
        return len(prepared)
    finally:
        dataframe.unpersist()


def save_custom_metadata_batch(results, spark, batch_num):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("column_guid", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("record_type", StringType(), True),
            StructField(
                "businessAttributes",
                MapType(StringType(), MapType(StringType(), StringType())),
                True,
            ),
        ]
    )
    records = [
        record
        for record in results
        if record.get("businessAttributes") and len(record.get("businessAttributes", {})) > 0
    ]
    if not records:
        return 0

    custom_data = []
    processed = set()
    for record in records:
        entity_id = record.get("id")
        column_guid = record.get("column_guid")
        unique_key = f"{entity_id}|{column_guid or 'table'}"
        if unique_key in processed:
            continue
        custom_data.append(
            {
                "id": entity_id,
                "column_guid": column_guid,
                "column_name": record.get("column_name"),
                "record_type": "column" if column_guid else "table",
                "businessAttributes": record.get("businessAttributes", {}),
            }
        )
        processed.add(unique_key)

    dataframe = (
        spark.createDataFrame(custom_data, schema=schema)
        .withColumn("data_snapshot", current_date())
        .dropDuplicates(["id", "column_guid"])
    )
    dataframe.persist()
    try:
        (
            dataframe.write.mode("append")
            .partitionBy("data_snapshot")
            .option("path", get_location_custom_attributes_table())
            .saveAsTable(CUSTOM_METADATA_TABLE)
        )
        return len(custom_data)
    finally:
        dataframe.unpersist()


def processar_bronze_completo_otimizado(max_assets=None, schemas_filter=None):
    start_time = time.time()
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("Nenhuma SparkSession ativa")

        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

        all_assets = get_all_assets_filtered(get_token(), get_purview_endpoint(), schemas_filter)
        unique_assets = list({asset["id"]: asset for asset in all_assets}.values())
        if max_assets and len(unique_assets) > max_assets:
            unique_assets = unique_assets[:max_assets]

        total_assets = len(unique_assets)
        batch_size = (total_assets + NUM_BATCHES - 1) // NUM_BATCHES if total_assets else 0
        batches = [
            unique_assets[index : index + batch_size]
            for index in range(0, total_assets, batch_size)
        ] if batch_size else []

        total_main_count = 0
        total_custom_count = 0
        for batch_num, batch in enumerate(batches, 1):
            results = process_assets_batch_optimized(batch, batch_num, len(batches))
            main_count, custom_count = save_batch_data_optimized(results, spark, batch_num)
            total_main_count += main_count
            total_custom_count += custom_count
            del results
            gc.collect()

        assets_sem_colunas_count = save_assets_sem_colunas(
            obter_e_limpar_assets_sem_colunas(), spark
        )
        logger.info(
            "Processamento concluído em %.1f minutos: assets=%s, principal=%s, custom=%s, sem_colunas=%s",
            (time.time() - start_time) / 60,
            len(unique_assets),
            total_main_count,
            total_custom_count,
            assets_sem_colunas_count,
        )
        return total_main_count + total_custom_count + assets_sem_colunas_count
    except Exception:
        logger.exception("Erro no processamento bronze")
        raise


if __name__ == "__main__":
    try:
        total_records = processar_bronze_completo_otimizado(max_assets=None, schemas_filter=None)
        logger.info("Total de registros processados: %s", total_records)
    except Exception:
        logger.exception("Falha ao processar bronze")
