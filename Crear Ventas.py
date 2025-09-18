import pandas as pd
import os
import numpy as np
# Esto se hace para simular un archivo con datos muy grandes
# Crear un directorio para los archivos CSV
if not os.path.exists("ventas_2023"):
    os.makedirs("ventas_2023")

# Generar datos ficticios para ventas
data_ventas = {
    "cliente_id": np.random.randint(0, 10000, size=20000),
    "fecha": pd.to_datetime(pd.date_range("2023-01-01", periods=20000)),
    "monto": np.random.rand(20000) * 1000,
}
df_ventas_pd = pd.DataFrame(data_ventas)

# Guardar en formato CSV
df_ventas_pd.to_csv("ventas_2023/ventas_parte_1.csv", index=False)
df_ventas_pd.to_csv("ventas_2023/ventas_parte_2.csv", index=False)
print("Archivos de ventas (CSV) creados en la carpeta 'ventas_2023/'.")
