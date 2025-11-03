"""
Utilidades para migración de MongoDB a PostgreSQL.
"""
import psycopg2
from pymongo import MongoClient
from datetime import datetime
from .rabbit_utils import wait_for_queue_empty


def mongo_productos_to_pg(mongo_uri, mongo_clean_db, pg_config, wait_for_rabbit_flag, rabbit_config):
    """
    Migra productos limpiados desde MongoDB a PostgreSQL.
    Si wait_for_rabbit_flag=True, espera a que la cola de RabbitMQ esté vacía (puede tomar ~15 hrs).
    
    Args:
        mongo_uri: URI de conexión a MongoDB
        mongo_clean_db: Nombre de la BD de productos limpios
        pg_config: Dict con configuración de PostgreSQL
            - host, port, dbname, user, password
        wait_for_rabbit_flag: Bool para esperar cola de RabbitMQ
        rabbit_config: Dict con configuración de RabbitMQ (para wait_for_queue_empty)
    """
    if wait_for_rabbit_flag:
        print(f"🐰 WAIT_FOR_RABBIT=True: Esperando cola 'productos_ids' (puede tomar varias horas)...")
        wait_for_queue_empty("productos_ids", rabbit_config)
    else:
        print(f"🐰 WAIT_FOR_RABBIT=False: Saltando espera de RabbitMQ, procediendo directamente a PostgreSQL")

    client = MongoClient(mongo_uri)
    db = client[mongo_clean_db]
    collection = db["clean_docs"]

    conn = psycopg2.connect(
        host=pg_config['host'],
        port=pg_config['port'],
        dbname=pg_config['dbname'],
        user=pg_config['user'],
        password=pg_config['password']
    )
    cur = conn.cursor()

    # Crear tabla si no existe
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS productos_clean_fields (
            _id TEXT PRIMARY KEY,
            nombre TEXT,
            marca TEXT,
            precio TEXT,
            referencia TEXT,
            cantidad TEXT,
            unidad TEXT,
            descripcion TEXT,
            fecha TEXT,
            categoria TEXT,
            pais TEXT,
            retail TEXT
        )
    """
    )

    for doc in collection.find({}):
        clean = doc.get("clean_fields", {})
        if not clean:
            continue

        # Campos desde clean_fields
        precio = clean.get("precio") or ""
        referencia = clean.get("referencia") or ""
        cantidad = clean.get("cantidad") or ""
        unidad = clean.get("unidad") or ""
        descripcion = clean.get("descripcion") or ""
        nombre = clean.get("nombre") or ""

        # Campos desde el root del documento
        marca = doc.get("marca") or ""
        pais = doc.get("pais") or ""
        retail = doc.get("retail") or ""
        fecha_scraping = doc.get("fecha_scraping")
        categoria = doc.get("producto") or ""

        # Si no hay fecha, usar actual
        if not fecha_scraping:
            fecha_scraping = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Inserción con manejo de conflicto
        cur.execute(
            """
            INSERT INTO productos_clean_fields (
                _id, nombre, marca, precio, referencia, cantidad, unidad,
                descripcion, fecha, categoria, pais, retail
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (_id) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                marca = EXCLUDED.marca,
                precio = EXCLUDED.precio,
                referencia = EXCLUDED.referencia,
                cantidad = EXCLUDED.cantidad,
                unidad = EXCLUDED.unidad,
                descripcion = EXCLUDED.descripcion,
                fecha = EXCLUDED.fecha,
                categoria = EXCLUDED.categoria,
                pais = EXCLUDED.pais,
                retail = EXCLUDED.retail
        """,
            (
                str(doc.get("_id")),
                nombre,
                marca,
                precio,
                referencia,
                cantidad,
                unidad,
                descripcion,
                fecha_scraping,
                categoria,
                pais,
                retail,
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Productos guardados en PostgreSQL con campos limpios y externos.")


def mongo_variables_to_pg(mongo_uri, mongo_clean_var_db, pg_config, wait_for_rabbit_flag, rabbit_config):
    """
    Migra variables limpiadas desde MongoDB a PostgreSQL.
    Si wait_for_rabbit_flag=True, espera a que la cola de RabbitMQ esté vacía (puede tomar ~15 hrs).
    
    Args:
        mongo_uri: URI de conexión a MongoDB
        mongo_clean_var_db: Nombre de la BD de variables limpias
        pg_config: Dict con configuración de PostgreSQL
            - host, port, dbname, user, password
        wait_for_rabbit_flag: Bool para esperar cola de RabbitMQ
        rabbit_config: Dict con configuración de RabbitMQ (para wait_for_queue_empty)
    """
    if wait_for_rabbit_flag:
        print(f"🐰 WAIT_FOR_RABBIT=True: Esperando cola 'variables_ids' (puede tomar varias horas)...")
        wait_for_queue_empty("variables_ids", rabbit_config)
    else:
        print(f"🐰 WAIT_FOR_RABBIT=False: Saltando espera de RabbitMQ, procediendo directamente a PostgreSQL")

    client = MongoClient(mongo_uri)
    db = client[mongo_clean_var_db]
    collection = db["clean_docs"]

    conn = psycopg2.connect(
        host=pg_config['host'],
        port=pg_config['port'],
        dbname=pg_config['dbname'],
        user=pg_config['user'],
        password=pg_config['password']
    )
    cur = conn.cursor()

    # Crear tabla si no existe
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS variables_clean_fields (
            _id TEXT PRIMARY KEY,
            nombre TEXT,
            valor TEXT,
            previous TEXT,
            unit TEXT,
            pais TEXT,
            fecha TEXT
        )
    """
    )

    for doc in collection.find({}):
        clean = doc.get("clean_fields", {})
        if not clean:
            continue

        valor = clean.get("Actual") or clean.get("Last") or ""
        previous = clean.get("Previous") or ""
        unit = clean.get("Unit") or ""
        nombre = doc.get("variable") or ""
        pais = doc.get("pais") or ""

        fecha = datetime.now().strftime("%y.%m.%d.%H")

        cur.execute(
            """
            INSERT INTO variables_clean_fields (_id, nombre, valor, previous, unit, pais, fecha)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (_id) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                valor = EXCLUDED.valor,
                previous = EXCLUDED.previous,
                unit = EXCLUDED.unit,
                pais = EXCLUDED.pais,
                fecha = EXCLUDED.fecha
        """,
            (str(doc.get("_id")), nombre, valor, previous, unit, pais, fecha),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(
        "✅ Variables guardadas en PostgreSQL con _id, nombre, valor, previous, unit, pais, fecha."
    )

