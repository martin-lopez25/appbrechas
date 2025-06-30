# -*- coding: utf-8 -*-
"""appbrechas2.ipynb"""

import polars as pl
import pandas as pd
import datetime
from dash import Dash, dcc, html, Input, Output, dash_table, State
from io import BytesIO

brecha = pl.read_csv("data/brechas_unificadas.csv")
catalogo_cargo = pl.read_csv("data/catalogo_cargo.csv")

# Renombrar columnas del catálogo
catalogo_cargo = catalogo_cargo.rename({
    catalogo_cargo.columns[0]: 'codigo_cnpm',
    catalogo_cargo.columns[1]: 'denominacion_del_puesto'
})

# Actualizar denominaciones de puestos desde el catálogo
brecha = brecha.drop('denominacion_del_puesto').join(
    catalogo_cargo,
    on="codigo_cnpm",
    how="left"
)

# Cambiar el nombre de la columna si viene como 'clasificacion_carga'
if 'clasificacion_carga' in brecha.columns:
    brecha = brecha.rename({'clasificacion_carga': 'clasificacion_cargo'})

# Convertir columnas numéricas
numeric_cols = ['total_ideal', 'total_real', 'pago_imb', 'brecha', 'excedente']
brecha = brecha.with_columns([
    pl.col(col).cast(pl.Float64).fill_null(0) for col in numeric_cols
])

# Clasificación de cargos
mapeo_clasificacion = {
    "CG": "Cuerpos de gobierno",
    "EN": "Enfermería",
    "ME": "Médicos especialistas",
    "MG": "Médicos generales",
    "OP": "Personal operativo",
    "FA": "Tradicional",
    "SIN_PUESTO": "Sin cargo"
}

brecha = brecha.with_columns(
    pl.col("codigo_cnpm").map_elements(
        lambda x: next((v for k, v in mapeo_clasificacion.items() if k in x), None),
        return_dtype=pl.String
    ).alias("clasificacion_cargo")
)

# Función para limpiar nombres (reemplazar _ por espacios y capitalizar)
def limpiar_nombres(texto):
    if texto is None:
        return ""
    return texto.replace("_", " ").title()

# Aplicar limpieza a nombres de puestos y columnas
brecha = brecha.with_columns(
    pl.col("denominacion_del_puesto").map_elements(limpiar_nombres)
)

# Inicializar la aplicación Dash
app = Dash(__name__, external_stylesheets=['https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600&display=swap'])
server = app.server

clues_list = brecha['clues_imb'].unique().sort().to_list()

app.layout = html.Div([
    html.Div([
        html.Img(
            src='https://framework-gb.cdn.gob.mx/landing/img/logoheader.svg',
            style={'height': '60px', 'margin-right': '20px', 'backgroundColor': '#105648', 'padding': '10px', 'borderRadius': '5px'}
        ),
        html.Img(
            src='https://imssbienestar.gob.mx/assets/img/logo_IB.svg',
            style={'height': '60px', 'backgroundColor': '#105648', 'padding': '10px', 'borderRadius': '5px'}
        )
    ], style={
        'display': 'flex',
        'justifyContent': 'center',
        'alignItems': 'center',
        'padding': '20px',
        'backgroundColor': '#105648',
        'borderBottom': '1px solid #e0e0e0'
    }),

    html.H1("Consulta de Brecha por CLUES",
            style={
                'textAlign': 'center',
                'marginTop': '10px',
                'marginBottom': '20px',
                'color': '#105648'
            }),

    html.Div([
        dcc.Dropdown(
            id='clues-dropdown',
            options=[{'label': f"{clues} - {brecha.filter(pl.col('clues_imb') == clues)['nombre_de_la_unidad'][0]}",
                      'value': clues} for clues in clues_list],
            placeholder="Selecciona un CLUES",
            style={
                'width': '100%',
                'fontFamily': 'Montserrat',
                'borderRadius': '5px',
                'border': '1px solid rgba(0, 0, 0, 0.1)'
            }
        ),
    ], style={'padding': '20px', 'backgroundColor': 'rgba(255, 255, 255, 0.85)', 'maxWidth': '800px', 'margin': '0 auto'}),

    html.Div(id='output-container', style={'backgroundColor': 'rgba(255, 255, 255, 0.85)', 'padding': '20px', 'maxWidth': '1200px', 'margin': '20px auto'}),

    html.Div([
        html.Button("Descargar Excel",
                   id='btn-download',
                   style={
                       'margin': '20px',
                       'backgroundColor': '#105648',
                       'color': '#e0c8ab',
                       'borderColor': '#0c231e',
                       'fontFamily': 'Montserrat',
                       'padding': '10px 20px',
                       'borderRadius': '5px',
                       'cursor': 'pointer',
                       'fontWeight': '600'
                   }),
        dcc.Download(id="download-dataframe-xlsx")
    ], style={'textAlign': 'center', 'backgroundColor': 'rgba(255, 255, 255, 0.85)'})
], style={'backgroundColor': '#FFF8E7', 'minHeight': '100vh', 'margin': '0', 'padding': '0', 'fontFamily': 'Montserrat'})

@app.callback(
    Output('output-container', 'children'),
    Input('clues-dropdown', 'value')
)
def update_output(clues_selected):
    if not clues_selected:
        return html.Div("Selecciona un CLUES para ver los resultados",
                        style={
                            'textAlign': 'center',
                            'padding': '20px',
                            'fontFamily': 'Montserrat'
                        })

    df_clues = brecha.filter(pl.col('clues_imb') == clues_selected)
    if df_clues.is_empty():
        return html.Div(f"No se encontró información para CLUES: {clues_selected}")

    unidad = df_clues['nombre_de_la_unidad'][0]
    entidad = df_clues['entidad'][0]
    fecha = datetime.datetime.now().strftime('%d/%m/%Y')

    # Resumen por clasificación con nombres formateados
    resumen_clasificacion = df_clues.group_by('clasificacion_cargo').agg([
        pl.col('total_ideal').sum().alias('Plantilla ideal'),
        pl.col('total_real').sum().alias('Ocupación'),
        pl.col('brecha').sum().alias('Brecha'),
        pl.col('excedente').sum().alias('Excedente')
    ]).sort('clasificacion_cargo')

    # Renombrar columna para visualización
    resumen_clasificacion = resumen_clasificacion.rename({
        'clasificacion_cargo': 'Clasificación'
    })

    # Top 5 con mayor brecha
    top_brecha = df_clues.sort('brecha', descending=True).head(5).select([
        pl.col('codigo_cnpm').alias('Código CNPM'),
        pl.col('denominacion_del_puesto').alias('Denominación del Cargo'),
        pl.col('total_ideal').alias('Plantilla ideal'),
        pl.col('total_real').alias('Ocupación'),
        pl.col('brecha').alias('Brecha')
    ])

    # Top 5 con mayor excedente
    top_excedente = df_clues.sort('excedente', descending=True).head(5).select([
        pl.col('codigo_cnpm').alias('Código CNPM'),
        pl.col('denominacion_del_puesto').alias('Denominación del Cargo'),
        pl.col('total_ideal').alias('Plantilla ideal'),
        pl.col('total_real').alias('Ocupación'),
        pl.col('excedente').alias('Excedente')
    ])

    # Top 5 médicos con mayor brecha
    top_medicos_brecha = df_clues.filter(
        pl.col('codigo_cnpm').str.starts_with('ME')
    ).sort('brecha', descending=True).head(5).select([
        pl.col('codigo_cnpm').alias('Código CNPM'),
        pl.col('denominacion_del_puesto').alias('Especialidad'),
        pl.col('total_ideal').alias('Plantilla ideal'),
        pl.col('total_real').alias('Ocupación'),
        pl.col('brecha').alias('Brecha')
    ])
    
    # Top 5 enfermeras con mayor brecha
    top_enfermeras_brecha = df_clues.filter(
        pl.col('codigo_cnpm').str.starts_with('EN')
    ).sort('brecha', descending=True).head(5).select([
        pl.col('codigo_cnpm').alias('Código CNPM'),
        pl.col('denominacion_del_puesto').alias('Denominación del Cargo'),
        pl.col('total_ideal').alias('plantilla ideal'),
        pl.col('total_real').alias('Ocupación'),
        pl.col('brecha').alias('Brecha')
    ])

    return html.Div([
        html.Div([
            html.H3(f"📋 REPORTE PARA: {unidad} ({clues_selected})",
                    style={
                        'textAlign': 'center',
                        'fontFamily': 'Montserrat',
                        'color': '#d2b992',
                        'backgroundColor': '#105648',
                        'padding': '15px 20px',
                        'borderRadius': '5px',
                        'marginBottom': '20px'
                    }),
            html.Div([
                html.P(f"📍 Entidad: {entidad}",
                       style={'fontFamily': 'Montserrat', 'color': '#105648', 'fontWeight': 'bold', 'margin': '5px 0'}),
                html.P(f"📅 Fecha de análisis: {fecha}",
                       style={'fontFamily': 'Montserrat', 'color': '#105648', 'fontWeight': 'bold', 'margin': '5px 0'}),
            ], style={'padding': '10px 20px', 'backgroundColor': '#f5f5f5', 'borderRadius': '5px', 'marginBottom': '20px'}),
        ]),

        html.Hr(style={'borderColor': '#105648', 'margin': '20px 0'}),

        html.Div([
            html.H4("🔍 DISTRIBUCIÓN POR CLASIFICACIÓN DE CARGOS",
                    style={
                        'fontFamily': 'Montserrat',
                        'color': '#fff',
                        'backgroundColor': '#105648',
                        'padding': '10px 15px',
                        'borderRadius': '5px',
                        'margin': '20px 0 10px 0'
                    }),
            dash_table.DataTable(
                data=resumen_clasificacion.to_dicts(),
                columns=[{'name': col.replace('_', ' ').title(), 'id': col} for col in resumen_clasificacion.columns],
                style_table={'overflowX': 'auto', 'backgroundColor': 'rgba(255, 255, 255, 0.95)', 'borderRadius': '5px'},
                style_cell={'padding': '10px', 'fontFamily': 'Montserrat', 'color': '#105648'},
                style_header={'backgroundColor': '#105648', 'color': '#d2b992', 'fontWeight': 'bold'},
                style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': 'rgba(16, 86, 72, 0.05)'}],
                page_size=10
            ),
        ]),

        html.Div([
            html.H4("🏥 TOP 5 PUESTOS EN MEDICINA DE ESPECIALIDAD CON MAYOR BRECHA",
                    style={
                        'fontFamily': 'Montserrat',
                        'color': '#fff',
                        'backgroundColor': '#105648',
                        'padding': '10px 15px',
                        'borderRadius': '5px',
                        'margin': '20px 0 10px 0'
                    }),
            dash_table.DataTable(
                data=top_medicos_brecha.to_dicts(),
                columns=[{'name': col, 'id': col} for col in top_medicos_brecha.columns],
                style_table={'overflowX': 'auto', 'backgroundColor': 'rgba(255, 255, 255, 0.95)', 'borderRadius': '5px'},
                style_cell={'padding': '10px', 'fontFamily': 'Montserrat', 'color': '#105648'},
                style_header={'backgroundColor': '#105648', 'color': '#d2b992', 'fontWeight': 'bold'},
                style_data_conditional=[
                    {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgba(16, 86, 72, 0.05)'},
                    {'if': {'column_id': 'Brecha'}, 'color': '#d62728', 'fontWeight': 'bold'}
                ],
                page_size=5
            ),
        ]),
        
        html.Div([
            html.H4("👩‍⚕️ TOP 5 ENFERMERÍA CON MAYOR BRECHA",
                    style={
                        'fontFamily': 'Montserrat',
                        'color': '#fff',
                        'backgroundColor': '#105648',
                        'padding': '10px 15px',
                        'borderRadius': '5px',
                        'margin': '20px 0 10px 0'
                    }),
            dash_table.DataTable(
                data=top_enfermeras_brecha.to_dicts(),
                columns=[{'name': col, 'id': col} for col in top_enfermeras_brecha.columns],
                style_table={'overflowX': 'auto', 'backgroundColor': 'rgba(255, 255, 255, 0.95)', 'borderRadius': '5px'},
                style_cell={'padding': '10px', 'fontFamily': 'Montserrat', 'color': '#105648'},
                style_header={'backgroundColor': '#105648', 'color': '#d2b992', 'fontWeight': 'bold'},
                style_data_conditional=[
                    {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgba(16, 86, 72, 0.05)'},
                    {'if': {'column_id': 'Brecha'}, 'color': '#d62728', 'fontWeight': 'bold'}
                ],
                page_size=5
            ),
        ]),

        html.Div([
            html.H4("📉 TOP 5 CARGOS CON MAYOR BRECHA",
                    style={
                        'fontFamily': 'Montserrat',
                        'color': '#fff',
                        'backgroundColor': '#105648',
                        'padding': '10px 15px',
                        'borderRadius': '5px',
                        'margin': '20px 0 10px 0'
                    }),
            dash_table.DataTable(
                data=top_brecha.to_dicts(),
                columns=[{'name': col, 'id': col} for col in top_brecha.columns],
                style_table={'overflowX': 'auto', 'backgroundColor': 'rgba(255, 255, 255, 0.95)', 'borderRadius': '5px'},
                style_cell={'padding': '10px', 'fontFamily': 'Montserrat', 'color': '#105648'},
                style_header={'backgroundColor': '#105648', 'color': '#d2b992', 'fontWeight': 'bold'},
                style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': 'rgba(16, 86, 72, 0.05)'}],
                page_size=5
            ),
        ]),

        html.Div([
            html.H4("📈 TOP 5 CARGOS CON MAYOR EXCEDENTE",
                    style={
                        'fontFamily': 'Montserrat',
                        'color': '#fff',
                        'backgroundColor': '#00aae4',
                        'padding': '10px 15px',
                        'borderRadius': '5px',
                        'margin': '20px 0 10px 0'
                    }),
            dash_table.DataTable(
                data=top_excedente.to_dicts(),
                columns=[{'name': col, 'id': col} for col in top_excedente.columns],
                style_table={'overflowX': 'auto', 'backgroundColor': 'rgba(255, 255, 255, 0.95)', 'borderRadius': '5px'},
                style_cell={'padding': '10px', 'fontFamily': 'Montserrat', 'color': '#105648'},
                style_header={'backgroundColor': '#105648', 'color': '#d2b992', 'fontWeight': 'bold'},
                style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': 'rgba(16, 86, 72, 0.05)'}],
                page_size=5
            ),
        ]),
    ])

@app.callback(
    Output("download-dataframe-xlsx", "data"),
    Input("btn-download", "n_clicks"),
    State('clues-dropdown', 'value'),
    prevent_initial_call=True
)
def download_excel(n_clicks, clues_selected):
    if not clues_selected:
        return None

    df_clues = brecha.filter(pl.col('clues_imb') == clues_selected)
    
    # Crear el resumen con la misma lógica de ordenamiento
    resumen = df_clues.group_by('clasificacion_cargo').agg([
        pl.col('total_ideal').sum().alias('Ideal'),
        pl.col('total_real').sum().alias('Ocupado'),
        pl.col('brecha').sum().alias('Brecha'),
        pl.col('excedente').sum().alias('Excedente'),
        # Agregar columna para ordenamiento
        pl.col('codigo_cnpm').first().alias('codigo_ejemplo')
    ]).with_columns(
        pl.when(pl.col('codigo_ejemplo').str.starts_with('ME'))
            .then(0)  # Prioridad más alta (ME)
            .when(pl.col('codigo_ejemplo').str.starts_with('EN'))
            .then(1)  # Segunda prioridad (EN)
            .otherwise(2)  # Todo lo demás
            .alias('orden_me')
    ).sort(['orden_me', 'clasificacion_cargo']).drop(['codigo_ejemplo', 'orden_me'])
    
    # Columnas para el detalle - incluyendo todas las brechas y excedentes por turno
    columnas_detalle = [
        'clasificacion_cargo',
        'codigo_cnpm',
        'denominacion_del_puesto',
        'total_ideal',
        'total_real',
        'pago_imb',
        'brecha',
        'excedente',
        'brecha_matutino',
        'brecha_vespertino',
        'brecha_nocturno',
        'brecha_jornada_acumulada',
        'excedente_matutino',
        'excedente_vespertino',
        'excedente_nocturno',
        'excedente_jornada_acumulada',
        'matutino',
        'Matutino B',
        'vespertino',
        'Nocturno A',
        'Nocturno B',
        'Jornada acumulada',
        'otro'
    ]
    
    # Filtrar solo las columnas que existen en el DataFrame
    columnas_existentes = [col for col in columnas_detalle if col in df_clues.columns]
    df_detalle = df_clues.with_columns(
        pl.when(pl.col('codigo_cnpm').str.starts_with('ME'))
        .then(0)  # Prioridad más alta (ME)
        .when(pl.col('codigo_cnpm').str.starts_with('EN'))
        .then(1)  # Segunda prioridad (EN)
        .otherwise(2)  # Todo lo demás
        .alias('orden_me')
    ).sort(['orden_me', 'codigo_cnpm']).select(columnas_existentes)

    output = BytesIO()
    
    resumen_pd = resumen.to_pandas()
    detalle_pd = df_detalle.to_pandas()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        resumen_pd.to_excel(writer, sheet_name='Resumen', index=False)
        detalle_pd.to_excel(writer, sheet_name='Detalle', index=False)

    return dcc.send_bytes(output.getvalue(), f"reporte_{clues_selected}.xlsx")

if __name__ == "__main__":
    app.run(debug=True)