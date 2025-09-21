# El código está diseñado para procesar grandes volúmenes de datos de clientes y ventas usando Dask,
# que permite trabajar con datasets que no caben en memoria (similar a Pandas pero distribuido).
# La finalidad es limpiar, enriquecer y analizar las ventas de 2023 por cliente, región y categoría.
import dask.dataframe as dd    #Importar Dask
import pandas as pd   #Importar pandas
from dask.diagnostics import ProgressBar  #Barra de progreso para ver como va la ejecucion ,funcion de dask

# Activar barra de progreso para ver ejecución
ProgressBar().register()

# 1. Cargar datos masivos
df_clientes = dd.read_parquet("clientes/*.parquet") #viene de archivos Parquet (formato columnar eficiente).
df_ventas = dd.read_csv("ventas_2023/*.csv", blocksize="128MB") #viene de múltiples CSV, pero se cargan por bloques de 128 MB para no saturar memoria.
 
# 2. Limpieza de datos
def limpiar_ventas(pdf):
    pdf["fecha"] = pd.to_datetime(pdf["fecha"], errors="coerce")
    pdf["monto"] = pdf["monto"].fillna(0)
    pdf = pdf[pdf["monto"] > 0]
    return pdf
#Convierte la columna fecha a tipo datetime (los errores se ponen como NaT).
#Rellena valores nulos en monto con 0.
#Filtra para quedarse solo con ventas de monto positivo.
df_ventas = df_ventas.map_partitions(limpiar_ventas, meta=df_ventas._meta)
#aplica la función a cada partición de Dask, como si fueran mini-DataFrames de Pandas.

# 3. Enriquecimiento (join con clientes)
df_full = df_ventas.merge(df_clientes, on="cliente_id", how="left")
# Hace un join entre ventas y clientes (enriquecimiento de datos), uniendo por cliente_id.
# El how="left" asegura que todas las ventas aparezcan, incluso si no hay cliente asociado.

# 4. Análisis avanzado

resumen = df_full.groupby(["region", "categoria"]).apply(
    lambda x: pd.Series({
        "monto_sum": x["monto"].sum(),  #suma total de ventas.
        "monto_mean": x["monto"].mean(), #promedio de ventas.
        "monto_count": x["monto"].count(), #número de ventas.
        "clientes_unicos": x["cliente_id"].nunique() #número de clientes distintos.
    })
).compute()  #ejecuta el grafo de tareas de Dask y devuelve un DataFrame de Pandas ya calculado.

# 5. Top categorías por región
top_categorias = (
    resumen.reset_index()
    .groupby("region")
    .apply(lambda x: x.nlargest(3, "monto_sum"))
)
#Encuentra las 3 categorías más vendidas por región, ordenadas por la suma de ventas (monto_sum).

# Imprime un encabezado y las primeras 10 filas del resumen general,
# que contiene las métricas por región y categoría
print("Resumen general:")
print(resumen.head(10))

# Imprime un encabezado y el resultado de top_categorias,
# es decir, las 3 categorías con mayor monto de ventas por cada región
print("\nTop categorías por región:")
print(top_categorias)
