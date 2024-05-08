import os
from dotenv import load_dotenv
import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime

def connect_to_redshift():
    try:
        # Conexión a Redshift
        load_dotenv()
        cnx = psycopg2.connect(
            dbname=os.getenv('DB_DBNAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        cursor = cnx.cursor()
        print("- Conexión a Redshift establecida correctamente")
        return cnx, cursor
    except psycopg2.Error as e:
        print("Error al conectar a Redshift:", e)
        return None, None

def create_table(cursor):
    try:
        # Crear tabla si no existe
        create_table_query = """
        CREATE TABLE IF NOT EXISTS quirogamariano_coderhouse.top_tracks (
            id INT IDENTITY(1,1) PRIMARY KEY,
            artist_name VARCHAR(255),
            track_name VARCHAR(255),
            top_position INT,
            date_added DATE
        )
        """
        cursor.execute(create_table_query)
        print("- Tabla creada exitosamente o ya existe.")
    except psycopg2.Error as e:
        print("Error al crear la tabla:", e)

def insert_track(cursor,artist_name,track_name,top_position,date_added):
    try:
        # Insertar datos
        insert_query = "INSERT INTO quirogamariano_coderhouse.top_tracks (artist_name,track_name,top_position,date_added) VALUES (%s, %s, %s, %s)"
        cursor.execute(insert_query, (artist_name,track_name,top_position,date_added))
    except psycopg2.Error as e:
        print("Error al insertar datos en la tabla:", e)

def connect_to_spotify():
    try:
        # Autenticación en Spotify
        load_dotenv()
        client_id = os.getenv('SP_CLIENT_ID')
        client_secret = os.getenv('SP_CLIENT_SECRET')

        client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

        print("- Conexión a Spotify establecida correctamente\n")
        return sp
    except Exception as error:
        print("Error al conectar a Spotify:", error)
        return None

def get_top_tracks_in_argentina(spotify_client, cursor):
    try:
        # "Top 50 Argentina" de Spotify
        top_tracks = spotify_client.playlist_tracks('37i9dQZEVXbMMy2roB9myp')

        print("==Top 50==")
        for index, track in enumerate(top_tracks['items'], start=1):
            track_name = track['track']['name']
            artist_name = track['track']['artists'][0]['name']
            top_position = index
            date_added = datetime.now().date()
            insert_track(cursor, artist_name, track_name, top_position, date_added)
            print(f"{top_position}. {artist_name} - {track_name}")

    except Exception as e:
        print("Error al obtener los tracks más escuchados en Argentina:", e)

cnx, cursor = connect_to_redshift()
spotify_client = connect_to_spotify()

if cnx and cursor and spotify_client:
    # Avanzo si ambas conexiones son exitosas
    try:
        create_table(cursor)
        get_top_tracks_in_argentina(spotify_client, cursor)
        cnx.commit()
    except Exception as e:
        print("Error al procesar datos de Spotify:", e)
        cnx.rollback() 
    finally:
        # Cierro conexión a Redshift
        cursor.close()
        cnx.close()
else:
    print("No se pudo completar el proceso debido a errores de conexión.")
