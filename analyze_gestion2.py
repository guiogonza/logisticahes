import pandas as pd
import numpy as np

# Leer la hoja GESTION
excel_path = r"data\ALMACENES\INDICADORES 2025.xlsx"
df = pd.read_excel(excel_path, sheet_name="GESTION ")

print("=" * 60)
print("ANÁLISIS DETALLADO DE GESTION")
print("=" * 60)

# Limpiar espacios
df.columns = df.columns.str.strip()
for col in df.select_dtypes(include=['object']).columns:
    df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]

print("\n1. DISTRIBUCIÓN POR SEDE Y TIPO INVENTARIO:")
pivot = df.groupby(['SEDE', 'TIPO INVENTARIO']).size().unstack(fill_value=0)
print(pivot)

print("\n2. DISTRIBUCIÓN POR RESPONSABLE:")
responsables = df['RESPONSABLE'].value_counts()
print(responsables)

print("\n3. INDICADORES:")
print(f"\nIndicador Inventario:")
print(df['Indicador Inventario'].value_counts())
print(f"\nIndicador respuesta:")
print(df['Indicador respuesta'].value_counts())

print("\n4. ANÁLISIS DE COLUMNAS NUMÉRICAS (DIAS y DIAS RESPUESTA):")
# Convertir DIAS a numérico
df['DIAS_NUM'] = pd.to_numeric(df['DIAS'], errors='coerce')
df['DIAS_RESPUESTA_NUM'] = pd.to_numeric(df['DIAS RESPUESTA'], errors='coerce')

print(f"\nDIAS - Estadísticas:")
print(f"  Promedio: {df['DIAS_NUM'].mean():.2f}")
print(f"  Min: {df['DIAS_NUM'].min():.0f}, Max: {df['DIAS_NUM'].max():.0f}")
print(f"  Valores con '-': {(df['DIAS'] == '-').sum()}")

print(f"\nDIAS RESPUESTA - Estadísticas:")
print(f"  Promedio: {df['DIAS_RESPUESTA_NUM'].mean():.2f}")
print(f"  Min: {df['DIAS_RESPUESTA_NUM'].min():.0f}, Max: {df['DIAS_RESPUESTA_NUM'].max():.0f}")
print(f"  Valores con '-': {(df['DIAS RESPUESTA'] == '-').sum()}")

print("\n5. POSIBLES MÉTRICAS PARA GRAFICAR:")
print("\nOpción 1: DIAS promedio por SEDE")
dias_por_sede = df[df['DIAS_NUM'].notna()].groupby('SEDE')['DIAS_NUM'].mean().sort_values(ascending=False)
print(dias_por_sede)

print("\nOpción 2: DIAS RESPUESTA promedio por RESPONSABLE")
dias_resp_por_responsable = df[df['DIAS_RESPUESTA_NUM'].notna()].groupby('RESPONSABLE')['DIAS_RESPUESTA_NUM'].mean().sort_values(ascending=False)
print(dias_resp_por_responsable)

print("\nOpción 3: Conteo de registros Dentro/Fuera del plazo por SEDE")
indicador_por_sede = df.groupby(['SEDE', 'Indicador respuesta']).size().unstack(fill_value=0)
print(indicador_por_sede)

print("\n6. RECOMENDACIÓN DE GRÁFICA:")
print("  - Barras agrupadas: SEDE vs DIAS promedio (inventario) y DIAS RESPUESTA promedio")
print("  - Barras apiladas: SEDE vs Indicador respuesta (Dentro/Fuera del plazo)")
print("  - Líneas: Evolución temporal de DIAS por MES")
