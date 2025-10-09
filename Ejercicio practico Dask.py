# El código está diseñado para procesar grandes volúmenes de datos de clientes y ventas usando Dask,
# que permite trabajar con datasets que no caben en memoria (similar a Pandas pero distribuido).

import dask.dataframe as dd    # Dask DataFrame: API parecida a pandas pero distribuida/paralela.
import pandas as pd           # Pandas: usado dentro de funciones que procesan particiones (p. ej. map_partitions).
from dask.diagnostics import ProgressBar  # Diagnostic: muestra una barra de progreso durante .compute()

# --- Barra de progreso ---
# Registrar/activar la barra de progreso para que, cuando ejecutemos operaciones que materializan el cálculo
# (por ejemplo .compute()), veamos una barra que indica el avance de las tareas.
ProgressBar().register()

# --- 1. Cargar datos masivos ---
# dd.read_parquet lee múltiples ficheros parquet en forma de Dask DataFrame sin cargar todo en memoria.
# Usualmente crea un grafo de tareas y carga por particiones cuando se ejecuta.
df_clientes = dd.read_parquet("clientes/*.parquet")  # Lee todos los parquet dentro de la carpeta/ patrón.

# dd.read_csv lee múltiples CSV y los divide en particiones basadas en blocksize para evitar OOM.
# blocksize="128MB" significa que cada partición (cada "pedazo" interno) intentará ser de ~128MB en disco.
# Esto ayuda a paralelizar y a trabajar con archivos más grandes que la RAM.
df_ventas = dd.read_csv("ventas_2023/*.csv", blocksize="128MB")

# --- 2. Limpieza de datos ---
# Definimos una función que será aplicada a cada partición (cada partición es un DataFrame tipo pandas).
def limpiar_ventas(pdf):
    # Convierte la columna "fecha" a datetime; errors="coerce" convierte valores inválidos en NaT.
    # Esto evita errores posteriores al trabajar con fechas y nos permite filtrar/ordenar por fecha.
    pdf["fecha"] = pd.to_datetime(pdf["fecha"], errors="coerce")

    # Rellena valores nulos en "monto" con 0. Esto evita errores al sumar/medianar cuando hay nulos.
    pdf["monto"] = pdf["monto"].fillna(0)

    # Filtramos filas con monto <= 0 (se queda solo con ventas "positivas").
    # Esto elimina registros no deseados como devoluciones anotadas de forma negativa
    # o registros con monto 0 que no aportan a métricas de ventas.
    pdf = pdf[pdf["monto"] > 0]

    # Devolver el DataFrame pandas ya limpio; Dask reconstruirá el DataFrame distribuido a partir de esto.
    return pdf

# map_partitions aplica la función 'limpiar_ventas' a cada partición (cada chunk/pedazo).
# El argumento meta indica la estructura (columnas y tipos) del DataFrame resultante para que
# Dask conozca el esquema sin ejecutar todo ahora. Usamos df_ventas._meta para mantener el mismo meta.
df_ventas = df_ventas.map_partitions(limpiar_ventas, meta=df_ventas._meta)
# Nota: es importante que 'meta' refleje las columnas y dtypes resultantes; si no, Dask puede lanzar errores.

# --- 3. Enriquecimiento (join con clientes) ---
# Realizamos un merge (join) entre ventas y clientes usando la columna 'cliente_id'.
# how="left" asegura que todas las ventas queden en el resultado aunque no exista información del cliente.
# Las columnas del DataFrame resultante incluirán las columnas de ventas + las columnas de clientes (sin duplicar cliente_id).
df_full = df_ventas.merge(df_clientes, on="cliente_id", how="left")
# Este merge es perezoso: no se ejecuta inmediatamente, sino que construye un grafo de tarea que se ejecutará al .compute().

# --- 4. Análisis avanzado ---
# Aquí agrupamos por región y categoría para calcular métricas agregadas por combinación (region, categoria).
# Usamos .apply con una función que devuelve una Serie por grupo: esto permite devolver múltiples métricas a la vez.
# IMPORTANTE: groupby.apply puede ser costoso en Dask (porque puede requerir reorganizar particiones).
resumen = df_full.groupby(["region", "categoria"]).apply(
    lambda x: pd.Series({
        "monto_sum": x["monto"].sum(),        # Suma total de 'monto' dentro del grupo.
        "monto_mean": x["monto"].mean(),      # Promedio de 'monto' dentro del grupo.
        "monto_count": x["monto"].count(),    # Conteo de ventas dentro del grupo.
        "clientes_unicos": x["cliente_id"].nunique()  # Número de clientes distintos en el grupo.
    })
).compute()  # .compute() materializa el resultado: ejecuta el grafo y trae un DataFrame de pandas a memoria.
# Al final 'resumen' es un pandas.DataFrame (no Dask), con índice multi-índice (region, categoria) y columnas con las métricas.

# --- 5. Top categorías por región ---
# Primero reseteamos el índice para convertir region/categoria en columnas normales.
# Luego agrupamos por 'region' y para cada región seleccionamos las 3 filas con mayor 'monto_sum'.
top_categorias = (
    resumen.reset_index()
    .groupby("region")
    .apply(lambda x: x.nlargest(3, "monto_sum"))
)
# Resultado: para cada región tendrás hasta 3 filas (las 3 categorías con mayor suma de ventas).

# --- Salidas / impresión ---
# Mostrar un encabezado y las primeras 10 filas del 'resumen' (pandas.DataFrame).
print("Resumen general:")
print(resumen.head(10))  # .head es barato ahora porque 'resumen' ya está en memoria.

# Imprime las top categorías por región (resultado de la selección de los 3 mejores por región).
print("\nTop categorías por región:")
print(top_categorias)
