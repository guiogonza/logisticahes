import pandas as pd

excel_path = r"data\ALMACENES\INDICADORES 2025.xlsx"
df = pd.read_excel(excel_path, sheet_name="GESTION ")

print("Columnas en el DataFrame:")
print(df.columns.tolist())

print("\n" + "="*60)
print("Analizando valores de DIAS y DIAS RESPUESTA:")

# Usar el nombre exacto de columna con espacios
col_dias = [c for c in df.columns if 'DIAS' in c and 'RESPUESTA' not in c][0]
col_dias_resp = [c for c in df.columns if 'DIAS RESPUESTA' in c][0]

print(f"\nColumna DIAS: '{col_dias}'")
print(f"\nValores únicos de '{col_dias}':")
print(df[col_dias].value_counts())

print(f"\n\nColumna DIAS RESPUESTA: '{col_dias_resp}'")
print(f"\nValores únicos de '{col_dias_resp}':")
print(df[col_dias_resp].value_counts())
