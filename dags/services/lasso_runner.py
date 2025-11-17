#!/usr/bin/env python3
"""
Script para ejecutar análisis LASSO y guardar resultados en PostgreSQL.
Copiado desde Lasso-service para ejecución directa dentro de Airflow.
"""
import os
import sys
import warnings

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score

warnings.filterwarnings("ignore")


def run_lasso():
    """Ejecuta el análisis LASSO y retorna el DataFrame de resultados."""
    PG_HOST = os.getenv("PG_HOST", "192.168.40.10")
    PG_PORT = os.getenv("PG_PORT", "8080")
    PG_DB = os.getenv("PG_DB", "mydb")
    PG_USER = os.getenv("PG_USER", "admin")
    PG_PASS = os.getenv("PG_PASS", "adminpassword")
    LASSO_TABLE = os.getenv("LASSO_TABLE", "lasso_results")

    print("=" * 60)
    print("🚀 INICIANDO ANÁLISIS LASSO")
    print("=" * 60)

    # ============================================================
    # PASO 1: Conexión a PostgreSQL
    # ============================================================
    print("\n--- PASO 1: Conexión a PostgreSQL ---")
    conn_str = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine = create_engine(conn_str)
    print(f" Conectado a: {PG_HOST}:{PG_PORT}/{PG_DB}")

    # ============================================================
    # PASO 2: Carga de datos desde PostgreSQL
    # ============================================================
    print("\n--- PASO 2: Carga de datos desde PostgreSQL ---")
    df_prices = pd.read_sql("SELECT * FROM tabla_precios", engine)
    df_macro = pd.read_sql("SELECT * FROM tabla_macroeconomicas", engine)
    print(f" Precios: {len(df_prices)} filas, {len(df_prices.columns)} columnas")
    print(f" Variables macro: {len(df_macro)} filas, {len(df_macro.columns)} columnas")

    # ============================================================
    # PASO 3: Normalización de columnas
    # ============================================================
    print("\n--- PASO 3: Normalización de columnas ---")
    df_prices.columns = df_prices.columns.str.lower().str.strip()
    for col in ["nombre", "retail", "pais", "marca", "producto", "moneda"]:
        if col in df_prices.columns:
            df_prices[col] = df_prices[col].astype(str).str.lower().str.strip()
    if "fecha" in df_prices.columns:
        df_prices["fecha"] = pd.to_datetime(df_prices["fecha"], errors="coerce")
    elif "fecha_scraping" in df_prices.columns:
        df_prices["fecha"] = pd.to_datetime(df_prices["fecha_scraping"], errors="coerce")

    df_macro.columns = df_macro.columns.str.lower().str.strip()
    for col in ["pais", "variables", "unidad"]:
        if col in df_macro.columns:
            df_macro[col] = df_macro[col].astype(str).str.lower().str.strip()
    if "fecha" in df_macro.columns:
        df_macro["fecha"] = pd.to_datetime(df_macro["fecha"], errors="coerce")
    print(" Columnas normalizadas")

    # ============================================================
    # PASO 4: Manejo de valores nulos
    # ============================================================
    print("\n--- PASO 4: Manejo de valores nulos ---")
    if "valor" in df_macro.columns:
        df_macro["valor"] = pd.to_numeric(df_macro["valor"], errors="coerce")
        df_macro["valor"] = df_macro["valor"].ffill()
        print(" Forward fill aplicado a variables macro")

    # ============================================================
    # PASO 5: Pivot de variables macro
    # ============================================================
    print("\n--- PASO 5: Pivot de variables macro ---")
    if "variables" in df_macro.columns and "valor" in df_macro.columns:
        df_macro_pivoted = (
            df_macro.pivot_table(
                index=["pais", "fecha"],
                columns="variables",
                values="valor",
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        print(f" Macro pivotado: {len(df_macro_pivoted)} filas, {len(df_macro_pivoted.columns)} columnas")
    else:
        df_macro_pivoted = df_macro.copy()
        print(" Macro ya está en formato pivotado")

    # ============================================================
    # PASO 6: Merge de precios con variables macro
    # ============================================================
    print("\n--- PASO 6: Merge de precios con variables macro ---")
    if "pais" in df_prices.columns:
        df_prices["pais"] = df_prices["pais"].astype(str).str.lower()
    if "pais" in df_macro_pivoted.columns:
        df_macro_pivoted["pais"] = df_macro_pivoted["pais"].astype(str).str.lower()

    df_merged = pd.merge(
        df_prices,
        df_macro_pivoted,
        on=["pais", "fecha"],
        how="left",
    )

    product_identifier = ["nombre", "marca", "retail", "producto"]
    df_merged = df_merged.sort_values(by=product_identifier + ["fecha"])
    print(f" Merge completado: {len(df_merged)} filas")

    # ============================================================
    # PASO 7: Feature Engineering - Cálculo de cambios porcentuales
    # ============================================================
    print("\n--- PASO 7: Feature Engineering - Cálculo de cambios porcentuales ---")
    target_col = "precio"
    macro_cols = [c for c in df_macro_pivoted.columns if c not in ["pais", "fecha"]]

    if target_col in df_merged.columns:
        df_merged[f"{target_col}_pct_change"] = df_merged.groupby(product_identifier)[
            target_col
        ].pct_change()
    for col in macro_cols:
        if col in df_merged.columns:
            df_merged[f"{col}_pct_change"] = df_merged.groupby("pais")[col].pct_change()

    df_final = df_merged.dropna()
    print(f" Cambios porcentuales calculados. Datos finales: {len(df_final)} filas")
    num_unique_products = df_final[product_identifier].drop_duplicates().shape[0]
    print(f" Productos únicos: {num_unique_products}")

    # ============================================================
    # PASO 8: Entrenamiento de modelos LASSO
    # ============================================================
    print("\n--- PASO 8: Entrenamiento de modelos LASSO ---")
    feature_cols = [
        f"{col}_pct_change"
        for col in macro_cols
        if f"{col}_pct_change" in df_final.columns
    ]
    if not feature_cols:
        raise RuntimeError("No se encontraron columnas de features para LASSO.")

    results = []
    min_obs = int(os.getenv("LASSO_MIN_OBS", "12"))

    for group_keys, group_df in df_final.groupby(product_identifier):
        if len(group_df) < min_obs:
            continue

        X = group_df[feature_cols]
        y = group_df[f"{target_col}_pct_change"]

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        lasso_cv = LassoCV(cv=5, random_state=42, n_jobs=-1)
        lasso_cv.fit(X_scaled, y)

        y_pred = lasso_cv.predict(X_scaled)
        r2 = r2_score(y, y_pred)

        entry = {
            "nombre": group_keys[0],
            "marca": group_keys[1],
            "retail": group_keys[2],
            "producto": group_keys[3],
            "r_squared": r2,
            "best_alpha": lasso_cv.alpha_,
            "n_obs": len(group_df),
        }

        for feature, coef in zip(feature_cols, lasso_cv.coef_):
            entry[f"coef_{feature.replace(' ', '_')}"] = coef
        results.append(entry)

    if not results:
        raise RuntimeError("No se generaron resultados de LASSO.")

    df_results = pd.DataFrame(results).sort_values(by="r_squared", ascending=False)
    print(f" Modelos entrenados: {len(df_results)}")
    print(f"   Top 5 R²: {df_results['r_squared'].head(5).tolist()}")

    # ============================================================
    # PASO 9: Guardado de resultados en PostgreSQL
    # ============================================================
    print(f"\n--- PASO 9: Guardado de resultados en PostgreSQL (tabla: {LASSO_TABLE}) ---")
    df_results.to_sql(LASSO_TABLE, engine, if_exists="replace", index=False)
    print(f" Resultados guardados: {len(df_results)} filas en tabla '{LASSO_TABLE}'")
    
    print("\n" + "=" * 60)
    print(" ANÁLISIS LASSO COMPLETADO EXITOSAMENTE")
    print("=" * 60)

    return df_results


if __name__ == "__main__":
    run_lasso()

