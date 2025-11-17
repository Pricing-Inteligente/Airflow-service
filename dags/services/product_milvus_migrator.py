# migrar_product_a_milv.py – migración de productos desde Postgres a Milvus
# • Columnas de origen/destino definidas en este archivo (constantes)
# • Conexiones Milvus/Postgres tomadas de variables de entorno (.env)
# • Embebe el texto canónico con SentenceTransformers (multilingual-e5 por defecto)

import os, re, json, hashlib
from datetime import datetime
from dateutil import parser as dtparser
from dotenv import load_dotenv
from tqdm import tqdm
import psycopg2, psycopg2.extras as pgx

from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility

# -------------------------------------------------------------
# Configuración específica de productos (columnas y colección)
# -------------------------------------------------------------

PG_PRODUCTS_TABLE = "tabla_precios"
PG_PRODUCTS_ID_COLUMN = "__auto__"  # usa "__auto__" para generar hash estable cuando no hay ID
PG_PRODUCTS_NAME_COLUMN = "nombre"
PG_PRODUCTS_BRAND_COLUMN = "marca"
PG_PRODUCTS_PRICE_COLUMN = "precio"
PG_PRODUCTS_DESC_COLUMN = "nombre"
PG_PRODUCTS_DATE_COLUMN = "fecha_scraping"
PG_PRODUCTS_CATEGORY_COLUMN = "producto"
PG_PRODUCTS_COUNTRY_COLUMN = "pais"
PG_PRODUCTS_STORE_COLUMN = "retail"
PG_PRODUCTS_URL_COLUMN = "url"
PG_PRODUCTS_CURRENCY_COLUMN = "moneda"
PG_PRODUCTS_UNIT_COLUMN = None     # reemplazar por nombre de columna si existe (ej. "unit")
PG_PRODUCTS_QTY_COLUMN = None      # reemplazar por nombre de columna si existe (ej. "qty")

MILVUS_COLLECTION_NAME = os.getenv("MILVUS_COLLECTION_PRODUCTOS", "products_latam")
PRIMARY_KEY_FIELD = "product_id"
VECTOR_FIELD = "embedding"
TEXT_FIELD = "canonical_text"
URL_FIELD = "url"
NAME_FIELD = "name"
VALUE_FIELD = "price"
BRAND_FIELD = "brand"
CATEGORY_FIELD = "category"
STORE_FIELD = "store"
COUNTRY_FIELD = "country"
UNIT_FIELD = "unit"
SIZE_FIELD = "size"
CURRENCY_FIELD = "currency"
LAST_SEEN_FIELD = "last_seen"

EMBED_MODEL_NAME = "intfloat/multilingual-e5-base"
MILVUS_METRIC = "IP"        # "IP" (Inner Product) equivale a cosine para embeddings normalizados
RECREATE_COLLECTION = False
DEBUG_SCHEMA_ORDER = False
HNSW_M = 16
HNSW_EF_CONSTRUCTION = 200
BATCH_SIZE = 1000

# -------------------------------------------------------------
# Utilidades
# -------------------------------------------------------------

def _parse_date_int(s: str) -> int:
    if not s:
        return 0
    s = str(s).strip()
    try:
        dt = dtparser.parse(s, dayfirst=True, fuzzy=True)
        return int(dt.strftime("%Y%m%d"))
    except Exception:
        pass
    # patrón especial d.M.y.H
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{2,4})\.(\d{1,2})$", s)
    if m:
        d, M, y, H = map(int, m.groups())
        if y < 100:
            y += 2000
        try:
            return int(datetime(y, M, d, H).strftime("%Y%m%d"))
        except Exception:
            return 0
    return 0


def _parse_number(text: str):
    if text is None:
        return None, None
    s = str(text).strip()
    sym = None
    m = re.search(r"(USD|EUR|ARS|COP|MXN|R\$|\$|€|£|₡)", s, re.I)
    if m:
        sym = m.group(1)
    raw = re.sub(r"[^0-9\.,-]", "", s)

    if raw.count(",") > 0 and raw.count(".") > 0:
        last = max(raw.rfind(","), raw.rfind("."))
        dec = raw[last]
        grp = "," if dec == "." else "."
        num = raw.replace(grp, "").replace(dec, ".")
    elif raw.count(",") > 0:
        parts = raw.split(",")
        if len(parts[-1]) in (1, 2):
            num = raw.replace(".", "").replace(",", ".")
        else:
            num = raw.replace(",", "")
    else:
        parts = raw.split(".")
        if len(parts[-1]) in (1, 2):
            num = raw
        else:
            num = raw.replace(".", "")
    try:
        return float(num), sym
    except Exception:
        return None, sym


def to_str(x, maxlen=None):
    if x is None:
        s = ""
    elif isinstance(x, (list, tuple, set)):
        s = " ".join("" if v is None else str(v) for v in x)
    elif isinstance(x, dict):
        s = json.dumps(x, ensure_ascii=False)
    else:
        s = str(x)
    s = s.strip()
    if maxlen and len(s) > maxlen:
        s = s[:maxlen]
    return s

# -------------------------------------------------------------
# Embeddings (E5 por defecto)
# -------------------------------------------------------------
_embedder = None

def embed_passages(texts):
    global _embedder
    if _embedder is None:
        from sentence_transformers import SentenceTransformer
        model_name = EMBED_MODEL_NAME
        _embedder = SentenceTransformer(model_name)
    texts = [f"passage: {t}" for t in texts]
    vecs = _embedder.encode(texts, normalize_embeddings=True)
    return vecs.tolist(), len(vecs[0])

# -------------------------------------------------------------
# Milvus helpers
# -------------------------------------------------------------

def _schema_field_names(col: Collection):
    return [f.name for f in col.schema.fields]


def reorder_to_schema(col: Collection, field_order, columns):
    schema_names = _schema_field_names(col)
    idx_map = {name: i for i, name in enumerate(field_order)}
    try:
        return [columns[idx_map[name]] for name in schema_names]
    except KeyError as e:
        raise KeyError(
            f"Campo ausente para reordenar: {e}. Schema={schema_names} / field_order={field_order}"
        )


def ensure_database():
    db_name = os.getenv("MILVUS_DB", "default")
    try:
        from pymilvus import db
        if db_name != "default":
            if db_name not in db.list_database():
                db.create_database(db_name)
            db.using_database(db_name)
            print(f"[INFO] Using Milvus database '{db_name}'")
        else:
            print("[INFO] Using Milvus database 'default'")
    except Exception as e:
        print(f"[WARN] Database namespaces not available or error: {e}")


def ensure_collection(dim, *, name, description, value_field_name="price"):
    vec_field = VECTOR_FIELD
    metric = MILVUS_METRIC.upper() if MILVUS_METRIC else "IP"

    recreate = RECREATE_COLLECTION
    if recreate and utility.has_collection(name):
        print(f"[INFO] Dropping existing collection '{name}'…")
        utility.drop_collection(name)

    if not utility.has_collection(name):
        fields = [
            FieldSchema(name=PRIMARY_KEY_FIELD, dtype=DataType.VARCHAR, is_primary=True, max_length=128),
            FieldSchema(name=vec_field, dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name=TEXT_FIELD, dtype=DataType.VARCHAR, max_length=4096),
            FieldSchema(name=URL_FIELD, dtype=DataType.VARCHAR, max_length=1024),
            FieldSchema(name=NAME_FIELD, dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name=value_field_name, dtype=DataType.DOUBLE),
            FieldSchema(name=BRAND_FIELD, dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name=CATEGORY_FIELD, dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name=STORE_FIELD, dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name=COUNTRY_FIELD, dtype=DataType.VARCHAR, max_length=64),
            FieldSchema(name=UNIT_FIELD, dtype=DataType.VARCHAR, max_length=64),
            FieldSchema(name=SIZE_FIELD, dtype=DataType.VARCHAR, max_length=128),
            FieldSchema(name=CURRENCY_FIELD, dtype=DataType.VARCHAR, max_length=32),
            FieldSchema(name=LAST_SEEN_FIELD, dtype=DataType.INT64),
        ]
        schema = CollectionSchema(fields=fields, description=description)
        col = Collection(name, schema)
        col.create_index(
            field_name=vec_field,
            index_params={
                "metric_type": metric,
                "index_type": "HNSW",
                "params": {"M": HNSW_M, "efConstruction": HNSW_EF_CONSTRUCTION},
            },
        )
        print(f"[INFO] Created collection '{name}' (metric={metric})")
    else:
        col = Collection(name)
        print(f"[INFO] Using existing collection '{name}'")

    col.load()
    if DEBUG_SCHEMA_ORDER:
        print("[DEBUG] schema_order =", _schema_field_names(col))
    return col


# Coerción segura de columnas a tipos del schema

def coerce_by_schema(col, field_order, columns):
    dtypes = {f.name: f.dtype for f in col.schema.fields}
    coerced = []
    for name, vals in zip(field_order, columns):
        dt = dtypes[name]
        if dt == DataType.VARCHAR:
            coerced.append([to_str(v) for v in vals])
        elif dt == DataType.DOUBLE:
            def to_float(x):
                if x is None:
                    return None
                if isinstance(x, (int, float)):
                    return float(x)
                if isinstance(x, list):
                    return float(x[0]) if x else None
                s = str(x).strip()
                if s.count(",") and s.count("."):
                    last = max(s.rfind(","), s.rfind("."))
                    dec = s[last]
                    grp = "," if dec == "." else "."
                    s = s.replace(grp, "").replace(dec, ".")
                elif s.count(",") == 1 and s.count(".") == 0:
                    s = s.replace(".", "").replace(",", ".")
                else:
                    s = s.replace(",", "")
                try:
                    return float(s)
                except:
                    return None
            coerced.append([to_float(v) for v in vals])
        elif dt == DataType.INT64:
            def to_int(x):
                if x is None:
                    return 0
                if isinstance(x, (int, float)):
                    return int(x)
                try:
                    return int(str(x))
                except:
                    return 0
            coerced.append([to_int(v) for v in vals])
        elif dt == DataType.FLOAT_VECTOR:
            coerced.append([[float(f) for f in v] for v in vals])
        else:
            coerced.append(list(vals))
    return coerced


def safe_insert(col, rows, field_order):
    if not rows:
        return 0, 0
    columns = [list(c) for c in zip(*rows)]
    columns = coerce_by_schema(col, field_order, columns)
    try:
        columns = reorder_to_schema(col, field_order, columns)
        col.insert(columns)
        return len(rows), 0
    except Exception as e:
        if len(rows) == 1:
            rid = rows[0][0]
            print(f"[SKIP] Row {rid} descartada: {e}")
            return 0, 1
        mid = len(rows) // 2
        ok1, sk1 = safe_insert(col, rows[:mid], field_order)
        ok2, sk2 = safe_insert(col, rows[mid:], field_order)
        return ok1 + ok2, sk1 + sk2


def insert_rows(col, rows, field_order):
    if not rows:
        return
    ok, skipped = safe_insert(col, rows, field_order)
    if skipped:
        print(f"[WARN] Insertadas OK: {ok}, saltadas: {skipped}")

# -------------------------------------------------------------
# Lectura Postgres
# -------------------------------------------------------------

def _sql_col(column_name):
    return "NULL" if column_name in (None, "") else column_name

# -------------------------------------------------------------
# Main
# -------------------------------------------------------------

def main():
    load_dotenv()

    # Conectar Milvus y Postgres
    connections.connect(host=os.getenv("MILVUS_HOST", "127.0.0.1"),
                        port=os.getenv("MILVUS_PORT", "19530"))
    ensure_database()

    pg = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", 5432)),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )
    cur = pg.cursor(cursor_factory=pgx.DictCursor)

    # dimensión de embeddings
    _, dim = embed_passages(["probar"])

    # Crear/usar colección de productos
    col_products = ensure_collection(
        dim,
        name=MILVUS_COLLECTION_NAME,
        description="LatAm products",
        value_field_name=VALUE_FIELD,
    )

    field_order = [
        PRIMARY_KEY_FIELD,
        VECTOR_FIELD,
        TEXT_FIELD,
        URL_FIELD,
        NAME_FIELD,
        VALUE_FIELD,
        BRAND_FIELD,
        CATEGORY_FIELD,
        STORE_FIELD,
        COUNTRY_FIELD,
        UNIT_FIELD,
        SIZE_FIELD,
        CURRENCY_FIELD,
        LAST_SEEN_FIELD,
    ]

    # ------------------------- Productos -------------------------
    id_expr_p = "NULL" if PG_PRODUCTS_ID_COLUMN in (None, "", "__auto__") else PG_PRODUCTS_ID_COLUMN
    unit_expr = _sql_col(PG_PRODUCTS_UNIT_COLUMN)
    qty_expr = _sql_col(PG_PRODUCTS_QTY_COLUMN)

    cur.execute(
        f"""
        SELECT {id_expr_p} AS id,
               {PG_PRODUCTS_NAME_COLUMN} AS name,
               {PG_PRODUCTS_BRAND_COLUMN} AS brand,
               {PG_PRODUCTS_PRICE_COLUMN} AS price,
               {PG_PRODUCTS_DESC_COLUMN} AS desc,
               {PG_PRODUCTS_DATE_COLUMN} AS date,
               {PG_PRODUCTS_CATEGORY_COLUMN} AS category,
               {PG_PRODUCTS_COUNTRY_COLUMN} AS country,
               {PG_PRODUCTS_STORE_COLUMN} AS store,
               {PG_PRODUCTS_URL_COLUMN} AS url,
               {PG_PRODUCTS_CURRENCY_COLUMN} AS currency,
               {unit_expr} AS unit,
               {qty_expr} AS qty
        FROM {PG_PRODUCTS_TABLE}
        """
    )
    productos = cur.fetchall()

    buf = []
    for r in tqdm(productos, desc="Migrando productos"):
        pid_raw = to_str(r.get("id"))[:128]
        if not pid_raw:
            # Genera ID estable cuando no existe en la tabla
            key = "|".join(
                [
                    to_str(r.get("country")), to_str(r.get("store")),
                    to_str(r.get("brand")), to_str(r.get("category")),
                    to_str(r.get("name")), to_str(r.get("url")),
                ]
            )
            pid = hashlib.md5(key.encode("utf-8")).hexdigest()
        else:
            pid = pid_raw

        name = to_str(r.get("name"), 512)
        brand = to_str(r.get("brand"), 256)
        category = to_str(r.get("category"), 256)
        unit = to_str(r.get("unit"), 64)          # puede no existir ⇒ ""
        size = to_str(r.get("qty"), 128)          # puede no existir ⇒ ""
        store = to_str(r.get("store"), 256)
        country = to_str(r.get("country"), 64)
        url = to_str(r.get("url"), 1024)
        desc = to_str(r.get("desc"), 2000)
        last_seen = _parse_date_int(r.get("date"))
        value_num, currency_sym = _parse_number(r.get("price"))
        if value_num is None:
            # No insertamos filas sin número válido
            continue
        if not currency_sym:
            currency_sym = to_str(r.get("currency"))

        canonical = " ".join(filter(None, [country, store, category, brand, name, desc]))
        vec, _ = embed_passages([canonical])

        row = [
            pid, vec[0], canonical, url, name, value_num, brand, category,
            store, country, unit, size, currency_sym or "", last_seen,
        ]
        buf.append(row)

        if len(buf) >= BATCH_SIZE:
            insert_rows(col_products, buf, field_order)
            buf = []

    insert_rows(col_products, buf, field_order)

    col_products.flush()
    try:
        utility.compact(col_products.name)
    except Exception:
        pass
    col_products.load()

    cur.close()
    pg.close()
    print("✅ Migración de productos completada.")


if __name__ == "__main__":
    main()
