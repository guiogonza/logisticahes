import pandas as pd
from pathlib import Path

excel_path = Path("data/ALMACENES/INDICADORES 2025.xlsx")
df = pd.read_excel(excel_path, sheet_name="PRO VS EJECU")

# Verificar tipo inventario (con espacio al final)
print("Columnas exactas:")
for c in df.columns:
    print(f"  |{c}|")

print(f"\nTipos de inventario únicos:")
print(df['TIPO INVENTARIO '].unique())

print(f"\nMeses únicos (FECHA PROPUESTA):")
print(df['FECHA PROPUESTA'].unique())

# Verificar datos
print("\nEstadísticas de PROGRAMADOS y EJECUTADOS:")
print(df[['PROGRAMADOS', 'EJECUTADOS', 'Indicador Programacion']].describe())

# Ver algunos registros
print("\nEjemplo de datos por tipo de inventario:")
for tipo in df['TIPO INVENTARIO '].unique():
    count = len(df[df['TIPO INVENTARIO '] == tipo])
    print(f"  {tipo}: {count} registros")
