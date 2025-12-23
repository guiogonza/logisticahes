"""
Script temporal para analizar la estructura del Excel de Indicadores
"""
import pandas as pd
from pathlib import Path

# Ruta al Excel
excel_path = Path(__file__).parent / "data" / "ALMACENES" / "INDICADORES 2025.xlsx"

print("=" * 80)
print("📊 ANÁLISIS DE INDICADORES 2025.xlsx")
print("=" * 80)

# Listar todas las hojas
try:
    xl = pd.ExcelFile(excel_path)
    print(f"\n📑 Hojas disponibles: {xl.sheet_names}")
    print(f"   Total de hojas: {len(xl.sheet_names)}")
    
    # Buscar la hoja OYMM
    if "OYMM" in xl.sheet_names:
        print("\n✅ Hoja 'OYMM' encontrada")
        
        # Leer la hoja OYMM
        df = pd.read_excel(excel_path, sheet_name="OYMM")
        
        print(f"\n📏 Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
        print(f"\n📋 Columnas disponibles:")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i}. {col}")
        
        print(f"\n🔍 Primeras 5 filas:")
        print(df.head())
        
        print(f"\n📊 Tipos de datos:")
        print(df.dtypes)
        
        print(f"\n🔢 Valores únicos en columnas clave:")
        # Buscar columnas que puedan ser filtros
        possible_filter_cols = ['MES', 'Mes', 'RESPONSABLE', 'Responsable', 'SEDE', 'Sede']
        for col in possible_filter_cols:
            if col in df.columns:
                unique_vals = df[col].nunique()
                print(f"   {col}: {unique_vals} valores únicos")
                if unique_vals < 20:
                    print(f"      Valores: {sorted(df[col].dropna().unique())}")
        
        print(f"\n💾 Información de valores nulos:")
        print(df.isnull().sum())
        
    else:
        print("\n❌ Hoja 'OYMM' NO encontrada")
        print(f"   Hojas disponibles: {xl.sheet_names}")
        
except Exception as e:
    print(f"\n❌ Error al analizar el archivo: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
