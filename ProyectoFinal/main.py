from dotenv import load_dotenv
import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import pandas as pd
from cryptography.fernet import Fernet
import base64
import os
import logging

# Configuración del registro de errores
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Cargar variables de entorno desde el archivo .env
load_dotenv()

with open('clave_proyectofinal.key', 'rb') as file:
    clave = file.read()

# Crear un objeto Fernet con la clave
cipher_suite = Fernet(clave)

# Función para descifrar los datos encriptados
def decrypt_data(encrypted_data):
    try:
        decrypted_data = cipher_suite.decrypt(base64.b64decode(encrypted_data)).decode()
        return decrypted_data
    except Exception as e:
        logging.error(f"Error al descifrar datos: {e}")
        return None

# Gestión de errores en la obtención de datos de las variables de entorno
def get_decrypted_data(env_variable):
    encrypted_data = os.getenv(env_variable)
    if encrypted_data:
        decrypted_data = decrypt_data(encrypted_data)
        if decrypted_data:
            return decrypted_data
    logging.error(f"No se pudo obtener o descifrar {env_variable}")
    return None

# Datos de conexión encriptados
DB_DBNAME_encrypted = get_decrypted_data('DB_DBNAME')
DB_USER_encrypted = get_decrypted_data('DB_USER')
DB_PASSWORD_encrypted = get_decrypted_data('DB_PASSWORD')
DB_HOST_encrypted = get_decrypted_data('DB_HOST')
DB_PORT_encrypted = get_decrypted_data('DB_PORT')
SP_CLIENT_ID_encrypted = get_decrypted_data('SP_CLIENT_ID')
SP_CLIENT_SECRET_encrypted = get_decrypted_data('SP_CLIENT_SECRET')

# Función para conectar a Redshift
def connect_to_redshift():
    try:
        cnx = psycopg2.connect(
            dbname=DB_DBNAME_encrypted,
            user=DB_USER_encrypted,
            password=DB_PASSWORD_encrypted,
            host=DB_HOST_encrypted,
            port=DB_PORT_encrypted
        )
        cursor = cnx.cursor()
        logging.info("Conexión a Redshift establecida correctamente")
        return cnx, cursor
    except psycopg2.Error as e:
        logging.error(f"Error al conectar a Redshift: {e}")
        return None, None

# Gestión de errores en la creación de tabla
def create_table(cursor):
    try:
        # Crear tabla si no existe
        create_table_query = """
        CREATE TABLE IF NOT EXISTS quirogamariano_coderhouse.top_tracks (
            id INT IDENTITY(1,1) PRIMARY KEY,
            artist_name VARCHAR(255),
            track_name VARCHAR(255),
            album_name VARCHAR(255),
            top_position INT,            
            playlist_count INT,
            date_added DATE
        )
        """
        cursor.execute(create_table_query)
        logging.info("Tabla creada exitosamente o ya existe.")
    except psycopg2.Error as e:
        logging.error(f"Error al crear la tabla: {e}")

# Gestión de errores en la inserción de datos
def insert_tracks(cursor, tracks):
    try:
        # Insertar datos
        insert_query = "INSERT INTO quirogamariano_coderhouse.top_tracks (artist_name, track_name, album_name, top_position, playlist_count, date_added) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.executemany(insert_query, tracks)
    except psycopg2.Error as e:
        logging.error(f"Error al insertar datos en la tabla: {e}")

# Función para conectar a Spotify
def connect_to_spotify():
    try:
        client_credentials_manager = SpotifyClientCredentials(SP_CLIENT_ID_encrypted, SP_CLIENT_SECRET_encrypted)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        logging.info("Conexión a Spotify establecida correctamente")
        return sp
    except Exception as e:
        logging.error(f"Error al conectar a Spotify: {e}")
        return None
    
# Gestión de errores en la obtención de los tracks
def get_top_tracks_in_argentina(spotify_client):
    try:
        # "Top 50 Argentina" de Spotify
        top_tracks = spotify_client.playlist_tracks('37i9dQZEVXbMMy2roB9myp')

        top_tracks_data = []
        logging.info("==Top 50==")
        for index, track in enumerate(top_tracks['items'], start=1):
            track_name = track['track']['name']
            artist_name = track['track']['artists'][0]['name']
            top_position = index            

            # Obtener el álbum de la pista
            album_id = track['track']['album']['id']
            album_info = spotify_client.album(album_id)
            album_name = album_info['name']

            # Cuento en la cantidad de listas que el tema esta en ellas
            search_query = f"track:\"{track_name}\" artist:\"{artist_name}\""
            search_results = spotify_client.search(q=search_query, type='track')
            playlist_count = search_results['tracks']['total']

            date_added = datetime.now().date()

            top_tracks_data.append((artist_name, track_name, album_name, top_position, playlist_count, date_added))

            logging.info(f"{top_position}. {artist_name} - {track_name} (Album: {album_name}) - Playlists: {playlist_count}")

        # DF para mostrar datos en consola
        df = pd.DataFrame(top_tracks_data, columns=['Artist Name', 'Track Name', 'Album Name', 'Top Position', 'Playlist Count', 'Date Added'])
        logging.info("\nMuestro datos insertados en DataFrame:")
        logging.info(df)

        return df

    except Exception as e:
        logging.error(f"Error al obtener los tracks más escuchados en Argentina: {e}")
        return None

cnx, cursor = connect_to_redshift()
spotify_client = connect_to_spotify()

if cnx and cursor and spotify_client:
    # Si ambas conexiones son exitosas continuo
    try:
        create_table(cursor)
        top_tracks_df = get_top_tracks_in_argentina(spotify_client)
        if not top_tracks_df.empty:
            insert_tracks(cursor, top_tracks_df.values.tolist())
            cnx.commit()
    except Exception as e:
        logging.error(f"Error al procesar datos de Spotify: {e}")
        cnx.rollback() 
    finally:
        # Cierro conexiones
        cursor.close()
        cnx.close()
else:
    logging.error("No se pudo completar el proceso debido a errores de conexión.")
