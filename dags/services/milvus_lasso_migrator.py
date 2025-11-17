"""
Migrador de resultados LASSO a Milvus.
Copiado desde Embedding-service para ejecutarse dentro de Airflow.
"""
import hashlib
import os
from typing import List, Tuple

import psycopg2
from psycopg2.extras import DictCursor
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)

LASSO_TABLE = os.getenv("PG_LASSO_TABLE", "lasso_results")

COL_NAME_SRC = os.getenv("PG_LASSO_NAME", "nombre")
COL_RETAIL_SRC = os.getenv("PG_LASSO_RETAIL", "retail")
COL_BRAND_SRC = os.getenv("PG_LASSO_BRAND", "marca")
COL_PRODUCT_SRC = os.getenv("PG_LASSO_PRODUCT", "producto")
COL_R2_SRC = os.getenv("PG_LASSO_R2", "r_squared")
COL_ALPHA_SRC = os.getenv("PG_LASSO_BEST_ALPHA", "best_alpha")
COL_N_OBS_SRC = os.getenv("PG_LASSO_N_OBS", "n_obs")
COL_COEF_DOLAR_SRC = os.getenv("PG_LASSO_COEF_DOLAR", "coef_cambio dolar_pct_change")
COL_COEF_CPI_SRC = os.getenv("PG_LASSO_COEF_CPI", "coef_cpi_pct_change")
COL_COEF_GDP_SRC = os.getenv("PG_LASSO_COEF_GDP", "coef_gdp_pct_change")
COL_COEF_GINI_SRC = os.getenv("PG_LASSO_COEF_GINI", "coef_gini_pct_change")
COL_COEF_INFLATION_SRC = os.getenv(
    "PG_LASSO_COEF_INFLATION", "coef_inflation rate_pct_change"
)
COL_COEF_INTEREST_SRC = os.getenv(
    "PG_LASSO_COEF_INTEREST", "coef_interest rate_pct_change"
)
COL_COEF_PPI_SRC = os.getenv(
    "PG_LASSO_COEF_PPI", "coef_producer prices_pct_change"
)

ALIAS_NAME = "nombre"
ALIAS_RETAIL = "retail"
ALIAS_BRAND = "marca"
ALIAS_PRODUCT = "producto"
ALIAS_R2 = "r_squared"
ALIAS_ALPHA = "best_alpha"
ALIAS_N_OBS = "n_obs"
ALIAS_COEF_DOLAR = "coef_cambio_dolar_pct_change"
ALIAS_COEF_CPI = "coef_cpi_pct_change"
ALIAS_COEF_GDP = "coef_gdp_pct_change"
ALIAS_COEF_GINI = "coef_gini_pct_change"
ALIAS_COEF_INFLATION = "coef_inflation_rate_pct_change"
ALIAS_COEF_INTEREST = "coef_interest_rate_pct_change"
ALIAS_COEF_PPI = "coef_producer_prices_pct_change"

MILVUS_HOST = os.getenv("MILVUS_HOST", "127.0.0.1")
MILVUS_PORT = os.getenv("MILVUS_PORT", "19530")
MILVUS_COLLECTION = os.getenv("MILVUS_LASSO_COLLECTION", "lasso_models")
MILVUS_RECREATE = os.getenv("MILVUS_RECREATE_COLLECTION", "1") == "1"

EMBED_MODEL = os.getenv("EMBED_MODEL", "intfloat/multilingual-e5-base")
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "128"))


def make_id(retail: str, brand: str, name: str, product: str) -> str:
    base = "|".join(
        [
            (retail or "").strip().lower(),
            (brand or "").strip().lower(),
            (name or "").strip().lower(),
            (product or "").strip().lower(),
        ]
    )
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def fetch_rows(conn) -> List[Tuple]:
    column_map = [
        (COL_NAME_SRC, ALIAS_NAME),
        (COL_RETAIL_SRC, ALIAS_RETAIL),
        (COL_BRAND_SRC, ALIAS_BRAND),
        (COL_PRODUCT_SRC, ALIAS_PRODUCT),
        (COL_R2_SRC, ALIAS_R2),
        (COL_ALPHA_SRC, ALIAS_ALPHA),
        (COL_N_OBS_SRC, ALIAS_N_OBS),
        (COL_COEF_DOLAR_SRC, ALIAS_COEF_DOLAR),
        (COL_COEF_CPI_SRC, ALIAS_COEF_CPI),
        (COL_COEF_GDP_SRC, ALIAS_COEF_GDP),
        (COL_COEF_GINI_SRC, ALIAS_COEF_GINI),
        (COL_COEF_INFLATION_SRC, ALIAS_COEF_INFLATION),
        (COL_COEF_INTEREST_SRC, ALIAS_COEF_INTEREST),
        (COL_COEF_PPI_SRC, ALIAS_COEF_PPI),
    ]
    cols_sql = ",".join(
        f'"{src}" AS "{alias}"' if src != alias else f'"{src}"'
        for src, alias in column_map
    )
    sql = f'SELECT {cols_sql} FROM "{LASSO_TABLE}"'
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()


def ensure_collection(dim: int = 768) -> Collection:
    if MILVUS_RECREATE and utility.has_collection(MILVUS_COLLECTION):
        print(f"[INFO] Dropping existing collection '{MILVUS_COLLECTION}'")
        utility.drop_collection(MILVUS_COLLECTION)

    if not utility.has_collection(MILVUS_COLLECTION):
        fields = [
            FieldSchema(
                name="lasso_id",
                dtype=DataType.VARCHAR,
                is_primary=True,
                max_length=64,
            ),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="nombre", dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name="retail", dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name="marca", dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name="producto", dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name="r_squared", dtype=DataType.DOUBLE),
            FieldSchema(name="best_alpha", dtype=DataType.DOUBLE),
            FieldSchema(name="n_obs", dtype=DataType.INT64),
            FieldSchema(name="coef_cambio_dolar_pct_change", dtype=DataType.DOUBLE),
            FieldSchema(name="coef_cpi_pct_change", dtype=DataType.DOUBLE),
            FieldSchema(name="coef_gdp_pct_change", dtype=DataType.DOUBLE),
            FieldSchema(name="coef_gini_pct_change", dtype=DataType.DOUBLE),
            FieldSchema(name="coef_inflation_rate_pct_change", dtype=DataType.DOUBLE),
            FieldSchema(name="coef_interest_rate_pct_change", dtype=DataType.DOUBLE),
            FieldSchema(name="coef_producer_prices_pct_change", dtype=DataType.DOUBLE),
            FieldSchema(
                name="canonical_text",
                dtype=DataType.VARCHAR,
                max_length=2048,
            ),
        ]
        schema = CollectionSchema(fields, description="Resultados de modelos LASSO")
        collection = Collection(MILVUS_COLLECTION, schema=schema)
        collection.create_index(
            field_name="embedding",
            index_params={
                "index_type": "HNSW",
                "metric_type": "IP",
                "params": {"M": 16, "efConstruction": 200},
            },
        )
        print(f"[INFO] Created collection '{MILVUS_COLLECTION}'")
    else:
        collection = Collection(MILVUS_COLLECTION)
        print(f"[INFO] Using existing collection '{MILVUS_COLLECTION}'")

    collection.load()
    return collection


def embed_texts(texts: List[str]) -> List[List[float]]:
    from sentence_transformers import SentenceTransformer
    import torch
    import numpy as np

    model = SentenceTransformer(
        EMBED_MODEL, device="cuda" if torch.cuda.is_available() else "cpu"
    )
    embeddings = model.encode(
        [f"passage: {t}" for t in texts],
        batch_size=EMBED_BATCH_SIZE,
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=True,
    )
    return embeddings.astype("float32").tolist()


def canonicalize(row) -> str:
    parts = [
        f"Nombre: {row.get(ALIAS_NAME)}",
        f"Retail: {row.get(ALIAS_RETAIL)}",
        f"Marca: {row.get(ALIAS_BRAND)}",
        f"Producto: {row.get(ALIAS_PRODUCT)}",
        f"R2: {row.get(ALIAS_R2)}",
        f"Alpha: {row.get(ALIAS_ALPHA)}",
        f"Observaciones: {row.get(ALIAS_N_OBS)}",
        f"Coef dólar: {row.get(ALIAS_COEF_DOLAR)}",
        f"Coef CPI: {row.get(ALIAS_COEF_CPI)}",
        f"Coef PIB: {row.get(ALIAS_COEF_GDP)}",
        f"Coef Gini: {row.get(ALIAS_COEF_GINI)}",
        f"Coef inflación: {row.get(ALIAS_COEF_INFLATION)}",
        f"Coef tasa interés: {row.get(ALIAS_COEF_INTEREST)}",
        f"Coef precios productor: {row.get(ALIAS_COEF_PPI)}",
    ]
    return " | ".join(str(p) for p in parts if p is not None)


def migrate():
    connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
    collection = ensure_collection(dim=768)

    pg_conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", 5432)),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )

    rows = fetch_rows(pg_conn)
    print(f"[INFO] Filas LASSO recuperadas: {len(rows)}")

    ids, nombres, retails, marcas, productos = [], [], [], [], []
    r2s, alphas, n_obs = [], [], []
    coeffs_dolar, coeffs_cpi, coeffs_gdp = [], [], []
    coeffs_gini, coeffs_infl, coeffs_interest, coeffs_ppi = [], [], [], []
    canonical_texts = []

    for row in rows:
        nombre = row.get(ALIAS_NAME)
        retail = row.get(ALIAS_RETAIL)
        marca = row.get(ALIAS_BRAND)
        producto = row.get(ALIAS_PRODUCT)

        rid = make_id(retail, marca, nombre, producto)
        ids.append(rid)

        nombres.append(str(nombre or ""))
        retails.append(str(retail or ""))
        marcas.append(str(marca or ""))
        productos.append(str(producto or ""))

        r2s.append(float(row.get(ALIAS_R2) or 0.0))
        alphas.append(float(row.get(ALIAS_ALPHA) or 0.0))
        n_obs.append(int(row.get(ALIAS_N_OBS) or 0))

        coeffs_dolar.append(float(row.get(ALIAS_COEF_DOLAR) or 0.0))
        coeffs_cpi.append(float(row.get(ALIAS_COEF_CPI) or 0.0))
        coeffs_gdp.append(float(row.get(ALIAS_COEF_GDP) or 0.0))
        coeffs_gini.append(float(row.get(ALIAS_COEF_GINI) or 0.0))
        coeffs_infl.append(float(row.get(ALIAS_COEF_INFLATION) or 0.0))
        coeffs_interest.append(float(row.get(ALIAS_COEF_INTEREST) or 0.0))
        coeffs_ppi.append(float(row.get(ALIAS_COEF_PPI) or 0.0))

        canonical_texts.append(canonicalize(row))

    embeddings = embed_texts(canonical_texts)

    batch_size = int(os.getenv("MILVUS_BATCH_SIZE", "2000"))
    total = len(ids)
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        slice_obj = slice(start, end)
        data = [
            ids[slice_obj],
            embeddings[slice_obj],
            nombres[slice_obj],
            retails[slice_obj],
            marcas[slice_obj],
            productos[slice_obj],
            r2s[slice_obj],
            alphas[slice_obj],
            n_obs[slice_obj],
            coeffs_dolar[slice_obj],
            coeffs_cpi[slice_obj],
            coeffs_gdp[slice_obj],
            coeffs_gini[slice_obj],
            coeffs_infl[slice_obj],
            coeffs_interest[slice_obj],
            coeffs_ppi[slice_obj],
            canonical_texts[slice_obj],
        ]
        collection.insert(data)
        print(f"[OK] Insertados {end}/{total}")

    collection.flush()
    collection.load()
    print("[DONE] Colección LASSO sincronizada en Milvus.")
    pg_conn.close()


if __name__ == "__main__":
    migrate()

