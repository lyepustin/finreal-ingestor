import json
import os
import pandas as pd
from datetime import datetime

# Ruta a la carpeta que contiene los archivos JSON
json_folder = 'data/manual_exports/ruralvia'
# Ruta donde se guardará el archivo CSV
csv_file_path = 'data/exports/20250611_163204_ruralvia_ahorro_menores_de_30_2176714216.csv'

# Lista para almacenar los datos
data = []

# Función para determinar la categoría basada en el código de origen
def get_category(codigo_origen):
    # Aquí puedes definir la lógica para asignar categorías
    if codigo_origen.startswith('AC'):
        return 'carga'
    elif codigo_origen.startswith('MD'):
        return 'tarjeta de débito'
    elif codigo_origen.startswith('TR'):
        return 'transferencia'
    elif codigo_origen.startswith('RZ'):
        return 'recibo'
    else:
        return 'otros'

# Iterar sobre los archivos en la carpeta
for filename in os.listdir(json_folder):
    if filename.endswith('.json'):
        file_path = os.path.join(json_folder, filename)
        with open(file_path, 'r') as file:
            json_data = json.load(file)
            movimientos = json_data['EE_O_UltimosMovimientosCuenta']['Respuesta']['ListaMovimientos']
            for entry in movimientos:
                # Extraer los campos relevantes y convertir a minúsculas
                date = entry.get('fecha')
                description = entry.get('concepto').lower()  # Convertir a minúsculas
                amount = entry.get('importe')
                balance = entry.get('saldoArrastre')
                category = get_category(entry.get('codigoOrigenApunte'))  # Obtener categoría

                # Formatear la fecha
                formatted_date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d %H:%M:%S")

                data.append({
                    'date': formatted_date,
                    'description': description,
                    'category': category,
                    'amount': amount,
                    'balance': balance
                })

# Crear un DataFrame de pandas
df = pd.DataFrame(data)

# Guardar el DataFrame en un archivo CSV
df.to_csv(csv_file_path, index=False)