#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilidades para análisis de resultados de benchmarks.

Este módulo contiene funciones para procesar logs de experimentos,
generar gráficas comparativas y calcular estadísticas de rendimiento.

Funciones disponibles:
- procesar_logs_multi_formato: Extrae datos de archivos .log
- renombrar_columnas: Normaliza nombres de columnas
- preparar_datos_para_grafica: Limpia y prepara datos numéricos
- generar_grafica_comparativa: Genera gráfico de barras comparativo
- generar_graficas_por_experimento_barras: Gráficos individuales por experimento
- calcular_medias_medianas: Calcula estadísticas descriptivas
- calcular_y_graficar_consistencia: Calcula y visualiza consistencia (diferencia media-mediana)

Autores: Manuela Ramos Ospina, Paula Andrea Pirela Rios, Carlos Eduardo Baez Coronado
"""

import os
import re
import ast
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


def procesar_logs_multi_formato(ruta_carpeta_principal):
    """
    Procesa archivos .log de múltiples experimentos y extrae métricas de rendimiento.

    Recorre recursivamente una carpeta con estructura:
        ruta_base/
        ├── cantidad_registros1/  (ej: 5k, 10k, 20k)
        │   ├── experimento1/     (ej: ex-python, ex-pandas)
        │   │   ├── tamaño1/      (ej: 5, 10, 15)
        │   │   │   └── output.log
        │   │   └── tamaño2/
        │   └── experimento2/
        └── cantidad_registros2/
    
    Args:
        ruta_carpeta_principal (str): Ruta raíz donde se encuentran los logs.
    
    Returns:
        pd.DataFrame: DataFrame con columnas:
            - Subcarpeta_1: Cantidad de registros (ej: '5k', '10k', '20k')
            - Subcarpeta_2: Nombre del experimento (ej: 'ex-python', 'ex-pandas')
            - Subcarpeta_3: Tamaño en GB (ej: '5', '10', '15')
            - Archivo_Origen: Nombre del archivo .log
            - Execution time: Tiempo de ejecución en segundos
            - '2', '4', '5': Conteos de códigos HTTP 2xx, 4xx, 5xx
    
    Example:
        >>> df = procesar_logs_multi_formato('./results')
        >>> print(df.head())
    """
    lista_registros = []
    ruta_norm = os.path.normpath(ruta_carpeta_principal)
    profundidad_base = len(ruta_norm.split(os.sep))

    PATRON_TIEMPO = r"Execution time:\s*(\d+\.\d+) seconds"

    for ruta_actual, directorios, archivos in os.walk(ruta_carpeta_principal):

        ruta_actual_norm = os.path.normpath(ruta_actual)
        componentes_ruta = ruta_actual_norm.split(os.sep)
        profundidad_actual = len(componentes_ruta)

        nombre_subcarpeta_1 = pd.NA
        nombre_subcarpeta_2 = pd.NA
        nombre_subcarpeta_3 = pd.NA

        if profundidad_actual > profundidad_base:
            nombre_subcarpeta_1 = componentes_ruta[profundidad_base]
        if profundidad_actual > profundidad_base + 1:
            nombre_subcarpeta_2 = componentes_ruta[profundidad_base + 1]
        if profundidad_actual > profundidad_base + 2:
            nombre_subcarpeta_3 = componentes_ruta[profundidad_base + 2]

        for nombre_archivo in archivos:

            if nombre_archivo.endswith('.log'):

                ruta_completa_archivo = os.path.join(ruta_actual, nombre_archivo)

                registro = {
                    'Subcarpeta_1': nombre_subcarpeta_1,
                    'Subcarpeta_2': nombre_subcarpeta_2,
                    'Subcarpeta_3': nombre_subcarpeta_3,
                    'Archivo_Origen': nombre_archivo
                }

                with open(ruta_completa_archivo, 'r', encoding='utf-8') as archivo:
                    contenido_completo = archivo.read()

                    match_tiempo = re.search(PATRON_TIEMPO, contenido_completo)
                    registro['Execution time'] = float(match_tiempo.group(1)) if match_tiempo else None

                    valores_match = re.search(r"(\[.*\]|\{.*\})", contenido_completo, re.DOTALL)

                    datos_extraidos = {}

                    if valores_match:
                        valores_str = valores_match.group(1).strip()
                        valores_str = valores_str.strip()

                        if valores_str.startswith('{') and valores_str.endswith('}'):
                            datos_extraidos = ast.literal_eval(valores_str)

                        elif valores_str.startswith('[') and valores_str.endswith(']'):
                            lista_de_tuplas = ast.literal_eval(valores_str)
                            datos_extraidos = {str(k): v for k, v in dict(lista_de_tuplas).items()}

                        registro.update(datos_extraidos)

                    lista_registros.append(registro)

    if lista_registros:
        df_final = pd.DataFrame(lista_registros)
        return df_final
    else:
        return pd.DataFrame()


def renombrar_columnas(df):
    """
    Renombra columnas del DataFrame a nombres más descriptivos y convierte a tipos numéricos.
    
    Args:
        df (pd.DataFrame): DataFrame con columnas originales de procesar_logs_multi_formato.
    
    Returns:
        pd.DataFrame: DataFrame con columnas renombradas:
            - cantidad_registros: Número de archivos procesados (ej: 5k, 10k, 20k)
            - experimento: Nombre del experimento (ej: ex-python, ex-pandas)
            - gigabytes: Tamaño del dataset en GB (ej: 5, 10, 15)
            - tiempo_de_ejecucion: Tiempo en segundos
    
    Example:
        >>> df_renamed = renombrar_columnas(df_logs)
    """
    df = df.rename(columns={
        'Subcarpeta_1': 'cantidad_registros',
        'Subcarpeta_2': 'experimento',
        'Subcarpeta_3': 'gigabytes',
        'Execution time': 'tiempo_de_ejecucion'
    })

    df['cantidad_registros'] = pd.to_numeric(df['cantidad_registros'], errors='coerce')
    df['gigabytes'] = pd.to_numeric(df['gigabytes'], errors='coerce')
    df['tiempo_de_ejecucion'] = pd.to_numeric(df['tiempo_de_ejecucion'], errors='coerce')

    return df


def preparar_datos_para_grafica(df):
    """
    Prepara los datos para visualización, limpiando y normalizando valores.
    
    Convierte notaciones como '5k', '10k', '20k' a valores numéricos (5000, 10000, 20000).
    
    Args:
        df (pd.DataFrame): DataFrame original de logs.
    
    Returns:
        pd.DataFrame: DataFrame con columna adicional 'cantidad_registros_num' (numérica).
    
    Example:
        >>> df_prep = preparar_datos_para_grafica(df_logs)
        >>> print(df_prep['cantidad_registros_num'].unique())
        [5000. 10000. 20000.]
    """
    df = df.rename(columns={
        'Subcarpeta_1': 'cantidad_registros',
        'Subcarpeta_2': 'experimento',
        'Subcarpeta_3': 'gigabytes',
        'Execution time': 'tiempo_de_ejecucion'
    })

    df['tiempo_de_ejecucion'] = pd.to_numeric(df['tiempo_de_ejecucion'], errors='coerce')

    def limpiar_cantidad_registros(val):
        if pd.isna(val):
            return np.nan
        val_str = str(val).lower().strip()
        if val_str.endswith('k'):
            try:
                return float(val_str[:-1]) * 1000
            except ValueError:
                return np.nan
        try:
            return float(val_str)
        except ValueError:
            return np.nan

    df['cantidad_registros_num'] = df['cantidad_registros'].apply(limpiar_cantidad_registros)

    return df


def generar_grafica_comparativa(df):
    """
    Genera un gráfico de barras comparando el tiempo promedio de ejecución entre experimentos.
    
    Args:
        df (pd.DataFrame): DataFrame con columnas 'experimento' y 'tiempo_de_ejecucion'.
    
    Returns:
        None: Muestra el gráfico con matplotlib.
    
    Example:
        >>> generar_grafica_comparativa(df_renamed)
        # Muestra gráfico de barras con tiempos promedio
    """
    df_promedio = df.groupby('experimento', as_index=False)['tiempo_de_ejecucion'].mean()

    plt.figure(figsize=(10, 6))

    ax = sns.barplot(
        x='experimento',
        y='tiempo_de_ejecucion',
        data=df_promedio,
        palette='Set1'
    )

    for p in ax.patches:
        ax.annotate(
            format(p.get_height(), '.2f'),  # Formato a 2 decimales
            (p.get_x() + p.get_width() / 2., p.get_height()),  # Posición (centro de la barra, altura)
            ha='center',
            va='center',
            xytext=(0, 9),  # Desplazamiento vertical para posicionar la etiqueta
            textcoords='offset points',
            fontsize=10
        )

    plt.title('Tiempo Promedio de Ejecución por Experimento', fontsize=14)
    plt.xlabel('Experimento', fontsize=12)
    plt.ylabel('Tiempo Promedio de Ejecución (segundos)', fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.6)

    plt.tight_layout()


def generar_graficas_por_experimento_barras(df):
    """
    Genera gráficos de barras individuales para cada experimento mostrando
    tiempo de ejecución vs. cantidad de registros.
    
    Args:
        df (pd.DataFrame): DataFrame preparado con 'cantidad_registros_num'.
    
    Returns:
        None: Muestra subplots con un gráfico por experimento.
    
    Example:
        >>> generar_graficas_por_experimento_barras(df_preparado)
        # Muestra grid de gráficos, uno por experimento
    """
    df_limpio = df.dropna(subset=['experimento', 'cantidad_registros_num', 'tiempo_de_ejecucion'])

    if df_limpio.empty:
        print("¡ALERTA! El DataFrame está vacío después de la limpieza. No se puede graficar.")
        return

    experimentos = df_limpio['experimento'].unique()
    num_experimentos = len(experimentos)

    cols = 2 if num_experimentos >= 2 else 1
    rows = int(np.ceil(num_experimentos / cols))

    fig, axes = plt.subplots(rows, cols, figsize=(8 * cols, 6 * rows))

    axes_flat = axes.flatten() if num_experimentos > 1 else [axes]

    fig.suptitle('Tiempo Promedio vs. Cantidad de Registros por Experimento', fontsize=16, y=1.02)

    for i, exp in enumerate(experimentos):
        ax = axes_flat[i]

        df_exp = df_limpio[df_limpio['experimento'] == exp]

        df_promedio_exp = df_exp.groupby('cantidad_registros_num', as_index=False)['tiempo_de_ejecucion'].mean()

        sns.barplot(
            x='cantidad_registros_num',
            y='tiempo_de_ejecucion',
            data=df_promedio_exp,
            ax=ax,
            palette='magma',
            order=sorted(df_promedio_exp['cantidad_registros_num'].unique())
        )

        for p in ax.patches:
            ax.annotate(
                format(p.get_height(), '.2f'),
                (p.get_x() + p.get_width() / 2., p.get_height()),
                ha='center',
                va='center',
                xytext=(0, 9),
                textcoords='offset points',
                fontsize=8
            )

        ax.set_title(f'Experimento: {exp}', fontsize=12)
        ax.set_xlabel('Cantidad de Registros', fontsize=10)
        ax.set_ylabel('Tiempo Promedio (segundos)', fontsize=10)
        ax.grid(axis='y', linestyle=':', alpha=0.5)

    for j in range(num_experimentos, len(axes_flat)):
        fig.delaxes(axes_flat[j])

    plt.tight_layout(rect=[0, 0.03, 1, 0.98])


def calcular_medias_medianas(df):
    """
    Calcula estadísticas descriptivas (media y mediana) del tiempo de ejecución por experimento.
    
    Args:
        df (pd.DataFrame): DataFrame con columnas 'experimento' y 'tiempo_de_ejecucion'.
    
    Returns:
        pd.DataFrame: DataFrame con columnas:
            - experimento (índice)
            - Tiempo_Medio (s): Media aritmética
            - Tiempo_Mediano (s): Mediana
            
        Ordenado de menor a mayor tiempo medio.
    
    Example:
        >>> stats = calcular_medias_medianas(df_preparado)
        >>> print(stats)
                          Tiempo_Medio (s)  Tiempo_Mediano (s)
        experimento                                      
        ex-duckdb                 12.45              11.23
        ex-polars                 15.67              14.89
    """
    df['tiempo_de_ejecucion'] = pd.to_numeric(df['tiempo_de_ejecucion'], errors='coerce')

    resultados = df.groupby('experimento')['tiempo_de_ejecucion'].agg(['mean', 'median'])
    # Agregar una columna de consistencia (diferencia entre media y mediana)
    resultados['Consistencia'] = abs(resultados['mean'] - resultados['median'])

    resultados = resultados.rename(columns={'mean': 'Tiempo_Medio (s)', 'median': 'Tiempo_Mediano (s)'})
    resultados = resultados.sort_values(by='Tiempo_Medio (s)', ascending=True)

    return resultados


def calcular_y_graficar_consistencia(df):
    """
    Calcula la consistencia (diferencia entre media y mediana) del tiempo de ejecución
    y genera un gráfico de barras para visualizar la variabilidad de cada experimento.
    
    Una diferencia pequeña indica que los tiempos son consistentes (poca variabilidad).
    Una diferencia grande indica alta variabilidad en los tiempos de ejecución.
    
    Args:
        df (pd.DataFrame): DataFrame con columnas 'experimento' y 'tiempo_de_ejecucion'.
    
    Returns:
        pd.DataFrame: DataFrame con columnas:
            - experimento (índice)
            - Tiempo_Medio (s): Media aritmética
            - Tiempo_Mediano (s): Mediana
            - Consistencia: Diferencia absoluta entre media y mediana
            - Consistencia_%: Porcentaje de variación respecto a la media
            
        Ordenado de más consistente (menor diferencia) a menos consistente.
    
    Example:
        >>> stats = calcular_y_graficar_consistencia(df_preparado)
        >>> print(stats)
        # Muestra tabla y gráfico de barras
                          Tiempo_Medio (s)  Tiempo_Mediano (s)  Consistencia  Consistencia_%
        experimento                                                                   
        ex-duckdb                 12.45              12.40          0.05          0.40%
        ex-polars                 15.67              14.20          1.47          9.38%
    """
    df['tiempo_de_ejecucion'] = pd.to_numeric(df['tiempo_de_ejecucion'], errors='coerce')

    # Calcular media y mediana
    resultados = df.groupby('experimento')['tiempo_de_ejecucion'].agg(['mean', 'median'])

    # Calcular consistencia (diferencia absoluta entre media y mediana)
    resultados['Consistencia'] = (resultados['mean'] - resultados['median'])
    
    # Renombrar columnas para mejor legibilidad
    resultados = resultados.rename(columns={
        'mean': 'Tiempo_Medio (s)', 
        'median': 'Tiempo_Mediano (s)'
    })

    # Crear gráfico de barras
    plt.figure(figsize=(10, 6))
    
    ax = sns.barplot(
        x=resultados.index,
        y='Consistencia',
        data=resultados.reset_index(),
        palette='viridis'
    )

    # Añadir etiquetas con valores
    for i, p in enumerate(ax.patches):
        consistencia_val = resultados.iloc[i]['Consistencia']
        ax.annotate(
            f'{consistencia_val:.2f}s',
            (p.get_x() + p.get_width() / 2., p.get_height()),
            ha='center',
            va='bottom',
            xytext=(0, 5),
            textcoords='offset points',
            fontsize=9
        )

    plt.title('Consistencia de Tiempos de Ejecución por Experimento\n(Diferencia entre Media y Mediana)', 
              fontsize=14, pad=20)
    plt.xlabel('Experimento', fontsize=12)
    plt.ylabel('Consistencia (segundos)', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.6)

    plt.tight_layout()

    return resultados
