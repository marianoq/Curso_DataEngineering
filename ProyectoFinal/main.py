from sqlalchemy import create_engine, text
from sqlalchemy.engine import url
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import pandas as pd
import logging
from config import DB_DBNAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, SP_CLIENT_ID, SP_CLIENT_SECRET
from tabulate import tabulate

# Conecto a la base de datos utilizando SQLAlchemy
def create_db_engine():
    try:
        db_url = url.URL.create(
            drivername="redshift+psycopg2",
            username=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=int(DB_PORT),
            database=DB_DBNAME
        )
        engine = create_engine(db_url)
        logging.info("Conexión a la base de datos establecida correctamente")
        return engine
    except Exception as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        return None

# Creación de tabla sino existe en mi esquema "quirogamariano_coderhouse"
def create_table(engine):
    try:
        with engine.connect() as connection:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS quirogamariano_coderhouse.top_tracks (
                id INT IDENTITY(1,1) PRIMARY KEY,
                artist_name VARCHAR(255),
                track_name VARCHAR(255),
                album_name VARCHAR(255),
                top_position INT,
                track_popularity INT,
                artist_genre VARCHAR(255),
                playlist_count INT,
                date_added DATE,
                spotify_id VARCHAR(255) UNIQUE
            )
            """
            connection.execute(text(create_table_query))
            logging.info("Tabla creada exitosamente o ya existe.")
    except Exception as e:
        logging.error(f"Error al crear la tabla: {e}")

# Conecto Spotify
def connect_to_spotify():
    try:
        client_credentials_manager = SpotifyClientCredentials(SP_CLIENT_ID, SP_CLIENT_SECRET)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        logging.info("Conexión a Spotify establecida correctamente")
        return sp
    except Exception as e:
        logging.error(f"Error al conectar a Spotify: {e}")
        return None

# Obtengo los de los tracks
def get_top_tracks_in_argentina(spotify_client):
    try:
        top_tracks = spotify_client.playlist_tracks('37i9dQZEVXbMMy2roB9myp')

        top_tracks_data = []
        for index, track in enumerate(top_tracks['items'], start=1):
            if 'track' in track and 'name' in track['track'] and 'artists' in track['track']:
                track_name = track['track']['name']
                artist_name = track['track']['artists'][0]['name']
                top_position = index

                album_id = track['track']['album']['id']
                album_info = spotify_client.album(album_id)
                album_name = album_info['name']

                track_id = track['track']['id']

                artist_id = track['track']['artists'][0]['id']
                artist_info = spotify_client.artist(artist_id)
                artist_genre = artist_info['genres'][0] if 'genres' in artist_info and artist_info['genres'] else "S/D"

                track_info = spotify_client.track(track_id)
                track_popularity = track_info['popularity'] if 'popularity' in track_info else None

                search_query = f"track:\"{track_name}\" artist:\"{artist_name}\""
                search_results = spotify_client.search(q=search_query, type='track')
                playlist_count = search_results['tracks']['total']

                date_added = datetime.now().date()

                top_tracks_data.append((artist_name, track_name, album_name, top_position, track_popularity, artist_genre, playlist_count, date_added, track_id))

        df = pd.DataFrame(top_tracks_data, columns=['artist_name', 'track_name', 'album_name', 'top_position', 'track_popularity', 'artist_genre', 'playlist_count', 'date_added', 'spotify_id'])
        return df

    except Exception as e:
        logging.error(f"Error al obtener los tracks más escuchados en Argentina: {e}")
        return None

# Lo uso para mostrar los datos en consola
def log_dataframe(df):
    # Filtramos solo las columnas deseadas
    filtered_df = df[['artist_name', 'track_name', 'top_position', 'spotify_id']]
    table_str = tabulate(filtered_df, headers='keys', tablefmt='psql')
    logging.info("\n\nDatos leidos en Spotify: \n%s", table_str)

# Función para insertar el DataFrame en la base de datos y devolver los datos insertados
def insert_dataframe_to_db(engine, df):
    inserted_rows = []
    try:
        with engine.connect() as connection:
            for _, row in df.iterrows():
                try:
                    existing_id = connection.execute(
                        text("SELECT id FROM quirogamariano_coderhouse.top_tracks WHERE spotify_id = :spotify_id"),
                        spotify_id=row['spotify_id']
                    ).fetchone()

                    if existing_id:
                        logging.info(f"ID de Spotify {row['spotify_id']} ya existe en la base de datos. Dato no insertado.")
                        continue

                    insert_query = text("""
                    INSERT INTO quirogamariano_coderhouse.top_tracks 
                    (artist_name, track_name, album_name, top_position, track_popularity, artist_genre, playlist_count, date_added, spotify_id)
                    VALUES (:artist_name, :track_name, :album_name, :top_position, :track_popularity, :artist_genre, :playlist_count, :date_added, :spotify_id)
                    """)
                    connection.execute(insert_query, 
                                       artist_name=row['artist_name'], 
                                       track_name=row['track_name'], 
                                       album_name=row['album_name'], 
                                       top_position=row['top_position'],
                                       track_popularity=row['track_popularity'],
                                       artist_genre=row['artist_genre'],  
                                       playlist_count=row['playlist_count'], 
                                       date_added=row['date_added'],
                                       spotify_id=row['spotify_id'])

                    inserted_rows.append(row)
                except Exception as e:
                    logging.error(f"Error al insertar fila {row}: {e}")
            logging.info("Datos insertados en la base de datos correctamente.")
    except Exception as e:
        logging.error(f"Error al insertar DataFrame en la base de datos: {e}")
    return pd.DataFrame(inserted_rows)

# Bloque principal de ejecución
def main():
    engine = create_db_engine()
    spotify_client = connect_to_spotify()

    if engine and spotify_client:
        try:
            create_table(engine)
            top_tracks_df = get_top_tracks_in_argentina(spotify_client)
            if top_tracks_df is not None and not top_tracks_df.empty:
                inserted_df = insert_dataframe_to_db(engine, top_tracks_df)
                if not inserted_df.empty:
                    log_dataframe(inserted_df)
        except Exception as e:
            logging.error(f"Error al procesar datos de Spotify: {e}")
    else:
        logging.error("No se pudo completar el proceso debido a errores de conexión.")

if __name__ == '__main__':
    main()
