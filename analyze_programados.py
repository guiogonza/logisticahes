import pandas as pd
from pathlib import Path

excel_path = Path("data/ALMACENES/INDICADORES 2025.xlsx")
df = pd.read_excel(excel_path, sheet_name="PRO VS EJECU")

print("Columnas encontradas:")
for c in df.columns:
    print(f"  {repr(c)}")

print(f"\nTotal registros: {len(df)}")

print("\nPrimeras 10 filas:")
print(df.head(10))

print("\nInformación de columnas:")
print(df.info())

# Ver valores únicos de columnas clave
if 'SEDE' in df.columns:
    print(f"\nSedes únicas: {df['SEDE'].unique()}")
if 'TIPO INVENTARIO' in df.columns:
    print(f"\nTipos de inventario: {df['TIPO INVENTARIO'].unique()}")
if 'MES' in df.columns:
    print(f"\nMeses únicos: {df['MES'].unique()}")
