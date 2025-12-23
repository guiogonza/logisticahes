import pandas as pd

# Leer la hoja GESTION
excel_path = r"data\ALMACENES\INDICADORES 2025.xlsx"
df = pd.read_excel(excel_path, sheet_name="GESTION ")

print("=" * 60)
print("ANÁLISIS DE HOJA GESTION")
print("=" * 60)
print(f"\nTotal de registros: {len(df)}")
print(f"\nColumnas ({len(df.columns)}):")
for i, col in enumerate(df.columns, 1):
    print(f"  {i}. {col}")

print(f"\nPrimeras 5 filas:")
print(df.head())

print(f"\nInformación de tipos de datos:")
print(df.dtypes)

print(f"\nValores únicos por columna:")
for col in df.columns:
    unique = df[col].nunique()
    print(f"  {col}: {unique} valores únicos")
    if unique <= 10:
        print(f"    Valores: {df[col].unique()[:10].tolist()}")

print(f"\nValores nulos:")
print(df.isnull().sum())
