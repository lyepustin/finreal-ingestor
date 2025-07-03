import json
import os
import pandas as pd
from datetime import datetime

# Ruta a la carpeta que contiene los archivos JSON
json_folder = 'data/manual_exports/bbva'
# Ruta donde se guardará el archivo CSV
csv_file_path = 'data/exports/20250611_173908_bbva_cuentas_personales_ES8001825319770200557238.csv'

# Lista para almacenar los datos
data = []

# Iterar sobre los archivos en la carpeta
for filename in os.listdir(json_folder):
    if filename.endswith('.json'):
        file_path = os.path.join(json_folder, filename)
        with open(file_path, 'r') as file:
            json_data = json.load(file)
            # Aquí debes ajustar la ruta según la estructura del JSON
            movimientos = json_data.get("accountTransactions", [])  # Cambia según la estructura real
            for entry in movimientos:
                # Extraer los campos relevantes
                date = entry.get('valueDate')
                description = entry.get("humanConceptName", "").strip().lower()  # Descripción en minúsculas
                more_info = entry.get("humanExtendedConceptName", "").strip().lower()  # Información adicional
                amount = float(entry.get('amount', {}).get('amount', 0.0))
                balance = float(entry.get('balance', {}).get('accountingBalance', {}).get('amount', 0.0))
                category = entry.get('humanCategory', {}).get('name', "Uncategorized").lower()  # Categoría en minúsculas

                # Formatear la fecha
                formatted_date = datetime.fromisoformat(date.replace('Z', '+00:00')).strftime("%Y-%m-%d %H:%M:%S")

                data.append({
                    'date': formatted_date,
                    'description': description,
                    'more_info': more_info,
                    'category': category,
                    'amount': amount,
                    'balance': balance
                })

# Crear un DataFrame de pandas
df = pd.DataFrame(data)

# Guardar el DataFrame en un archivo CSV
df.to_csv(csv_file_path, index=False)