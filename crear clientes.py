import pandas as pd
import os

# Crear un directorio para los archivos Parquet
if not os.path.exists("clientes"):
    os.makedirs("clientes")

# Generar datos ficticios para clientes
data_clientes = {
    "cliente_id": range(10000),
    "region": ["Region_A", "Region_B"] * 5000,
    "categoria": ["Cat_X", "Cat_Y", "Cat_Z"] * 3333 + ["Cat_X"]
}
df_clientes_pd = pd.DataFrame(data_clientes)

# Guardar en formato Parquet (esto crea múltiples archivos)
df_clientes_pd.to_parquet("clientes/clientes_part_0.parquet")
df_clientes_pd.to_parquet("clientes/clientes_part_1.parquet")
print("Archivos de clientes (Parquet) creados en la carpeta 'clientes/'.")