import pandas as pd
from pathlib import Path

excel_path = Path("data/ALMACENES/INDICADORES 2025.xlsx")
df = pd.read_excel(excel_path, sheet_name="BRIGADAS ")

print("Columnas encontradas:")
for c in df.columns:
    print(f"  {repr(c)}")

print(f"\nTotal registros: {len(df)}")
print(f"\nSEDES: {df['SEDE '].unique()}")
print(f"\nMESES: {df['MES '].unique()[:10]}")
print(f"\nESTADOS: {df['ESTADO'].unique()}")

print("\nPrimeras 3 filas:")
print(df.head(3))

# Calcular DESVIACION si no existe
if 'DESVIACION' not in df.columns:
    print("\nCalculando DESVIACION...")
    # DESVIACION = (COSTO DIFERENCIA / COSTO TOTAL) * 100
    df['DESVIACION'] = (df['COSTO DIFERENCIA '] / df['COSTO TOTAL']) * 100
    df['DESVIACION'] = df['DESVIACION'].fillna(0)
    print("DESVIACION calculada")
    print(df[['COSTO TOTAL', 'COSTO DIFERENCIA ', 'DESVIACION']].head())
