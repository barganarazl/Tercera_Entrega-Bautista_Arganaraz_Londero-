# Importación de librerías a utilizar:

import os
import datetime
import json
import pandas as pd
import requests as req
import psycopg2 as pg2
from psycopg2.extras import execute_values

# Seteo de variables presentes en el archivo 'variables.json':

current_dir = os.path.dirname(os.path.abspath(__file__))
json_file_path = os.path.join(current_dir, "variables.json")

with open(json_file_path) as json_file:
        variables_json = json.load(json_file)

nombres = variables_json["Nombres_Empresas"]
columnas_df = variables_json["Columnas_DataFrame"]
dict_aws = variables_json["Columnas_Redshift"]
list_aws = list(variables_json["Columnas_Redshift"].keys())
credenciales = variables_json["Credenciales"]

CLAVE_API = credenciales["API_KEY"]
USUARIO_BD = credenciales["DB_USER"]
CONTRASENA_BD = credenciales["DB_PASSWORD"]
HOST_BD = credenciales["DB_HOST"]
PUERTO_BD = credenciales["DB_PORT"]
NOMBRE_BD = credenciales["DB_NAME"]


# Extracción desde la API de los datos diarios de "Los 7 Magníficos" (Alphabet, Amazon, Apple, Meta, Microsoft, Nvidia y Tesla) correspondiente al NASDAQ tomando en cuenta el último año (sin contar Sabados y Domingos):

def get_cotizaciones():
    try:
        api = 'https://api.twelvedata.com/time_series?symbol=GOOGL,AMZN,AAPL,META,MSFT,NVDA,TSLA&exchange=NASDAQ&interval=1day&format=JSON&outputsize=260&apikey=' + CLAVE_API
        peticion = req.get(api)
        print('Conexión exitosa a la API y obtención de los datos.')
    except Exception as e:
        print('Conexión fallida a la API y obtención de los datos.')
        print(e)

    # Conversión de datos a formato JSON:

    datos_json = peticion.json()

    # Normalización de datos y agregado de columnas:

    datos_df = pd.DataFrame()
    claves = list(datos_json.keys())
    contador = 0

    for empresa in datos_json:
        tabla = pd.json_normalize(datos_json[empresa]['values'])
        tabla['Codigo'] = claves[contador]
        tabla['Empresa'] = nombres[claves[contador]]
        datos_df = pd.concat([datos_df, tabla], axis = 0)
        contador += 1

    datos_df.columns = columnas_df

    datos_df = datos_df.drop_duplicates()
    datos_df = datos_df.sort_values(by = ['Dia', 'Empresa'], ascending = [False, True],ignore_index = True)

    datos_df['Clave_Compuesta'] = datos_df.Codigo.str.cat(datos_df.Dia)
    datos_df['Columna_Temporal'] = str(datetime.datetime.now())

    return(datos_df.to_json())


# Conexión a la base de datos en Amazon Redshift y creación del cursor:

def conexion_redshift():
    try:
        conexion = pg2.connect(host = HOST_BD, port = PUERTO_BD, dbname = NOMBRE_BD, user = USUARIO_BD, password = CONTRASENA_BD)
        print('Conexión exitosa a la base de datos para la creación de las tablas Staging y Destino.')
    except Exception as e:
        print('Conexión fallida a la base de datos para la creación de las tablas Staging y Destino.')
        print(e)

    # Creación de las tablas principal y staging en Amazon Redshift:

    columnas_query_create = ''
    contador_create = 1

    for i in dict_aws:
            if contador_create != len(dict_aws):
                    columnas_query_create = columnas_query_create + i + ' ' + dict_aws[i] + ','
            else:
                    columnas_query_create = columnas_query_create + i + ' ' + dict_aws[i]
            contador_create += 1

    query_create = 'CREATE TABLE IF NOT EXISTS b_arganaraz_londero_coderhouse.cotizacion_magnificos(' + columnas_query_create + ');' + '''

    CREATE TABLE IF NOT EXISTS b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging(''' + columnas_query_create + ');'

    try:
        with conexion.cursor() as cursor:
            cursor.execute(query_create)
        print('Creación correcta de las tablas Staging y Destino.')
    except Exception as e:
        print('Creación fallida de las tablas Staging y Destino.')
        print(e)
        
    conexion.commit()
    conexion.close


    # Insercion de datos a la tabla staging creada en Amazon Redshift:

def insercion_datos(cotizaciones_json):
    try:
        conexion = pg2.connect(host = HOST_BD, port = PUERTO_BD, dbname = NOMBRE_BD, user = USUARIO_BD, password = CONTRASENA_BD)
        print('Conexión exitosa a la base de datos para la inserción de los datos en la tabla.')
    except Exception as e:
        print('Conexión fallida a la base de datos para la inserción de los datos en la tabla.')
        print(e)

    columnas_query_insert = ''
    contador_insert = 1

    for x in dict_aws:
            if contador_insert != len(dict_aws):
                    columnas_query_insert = columnas_query_insert + x + ', '
            else:
                    columnas_query_insert = columnas_query_insert + x
            contador_insert += 1

    query_insert = 'INSERT INTO b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging(' + columnas_query_insert + ') VALUES %s;'

    datos_dict = cotizaciones_json
    datos_df = pd.DataFrame(eval(datos_dict))

    valores = [tuple(var) for var in datos_df[list_aws].to_numpy()]

    try:
        with conexion.cursor() as cursor:
            execute_values(cursor, query_insert, valores)
        print('Inserción correcta de los datos en la tabla Staging.')
    except Exception as e:
        print('Inserción fallida de los datos en la tabla Staging.')
        print(e)

    conexion.commit()

    # Actualización incremental de la tabla principal en base a los datos de la tabla staging en Amazon Redshift:

    query_incremental = '''
    DELETE FROM b_arganaraz_londero_coderhouse.cotizacion_magnificos 
    USING b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging 
    WHERE b_arganaraz_londero_coderhouse.cotizacion_magnificos.Clave_Compuesta = b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging.Clave_Compuesta;

    INSERT INTO b_arganaraz_londero_coderhouse.cotizacion_magnificos SELECT * FROM b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging;

    DROP TABLE b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging;
    '''

    try:
        with conexion.cursor() as cursor:
            cursor.execute(query_incremental)
        print('Inserción correcta de los datos en la tabla Destino a partir de la tabla Staging.')
    except Exception as e:
        print('Inserción fallida de los datos en la tabla Destino a partir de la tabla Staging.')
        print(e)
    
    conexion.commit()
    conexion.close()
    
# Verificación del entorno en el que se encuentra ejecutando el archivo:
    
if os.environ.get("ENTORNO") == 'Docker':
    conexion_redshift()
    insercion_datos(get_cotizaciones())