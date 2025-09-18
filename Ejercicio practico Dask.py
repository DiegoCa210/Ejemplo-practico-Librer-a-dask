import dask.dataframe as dd
import pandas as pd
from dask.diagnostics import ProgressBar

# Activar barra de progreso para ver ejecución
ProgressBar().register()

# 1. Cargar datos masivos
df_clientes = dd.read_parquet("clientes/*.parquet")
df_ventas = dd.read_csv("ventas_2023/*.csv", blocksize="128MB")

# 2. Limpieza de datos
def limpiar_ventas(pdf):
    pdf["fecha"] = pd.to_datetime(pdf["fecha"], errors="coerce")
    pdf["monto"] = pdf["monto"].fillna(0)
    pdf = pdf[pdf["monto"] > 0]
    return pdf

df_ventas = df_ventas.map_partitions(limpiar_ventas, meta=df_ventas._meta)

# 3. Enriquecimiento (join con clientes)
df_full = df_ventas.merge(df_clientes, on="cliente_id", how="left")

# 4. Análisis avanzado
resumen = df_full.groupby(["region", "categoria"]).apply(
    lambda x: pd.Series({
        "monto_sum": x["monto"].sum(),
        "monto_mean": x["monto"].mean(),
        "monto_count": x["monto"].count(),
        "clientes_unicos": x["cliente_id"].nunique()
    })
).compute()

# 5. Top categorías por región
top_categorias = (
    resumen.reset_index()
    .groupby("region")
    .apply(lambda x: x.nlargest(3, "monto_sum"))
)

print("Resumen general:")
print(resumen.head(10))
print("\nTop categorías por región:")
print(top_categorias)