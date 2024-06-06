from dotenv import load_dotenv
import os
import logging
from cryptography.fernet import Fernet
import base64

# Registro de errores
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Cargo la variable de entorno desde el archivo .env
load_dotenv()

# Leo clave de cifrado
#try:
#    with open('clave_proyectofinal.key', 'rb') as file:
#        clave = file.read()
#except FileNotFoundError:
#    logging.error("Archivo de clave no encontrado.")
#    raise

clave = os.getenv("KEY")

# Creo el objeto Fernet con la clave
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
DB_DBNAME = get_decrypted_data('DB_DBNAME')
DB_USER = get_decrypted_data('DB_USER')
DB_PASSWORD = get_decrypted_data('DB_PASSWORD')
DB_HOST = get_decrypted_data('DB_HOST')
DB_PORT = get_decrypted_data('DB_PORT')
SP_CLIENT_ID = get_decrypted_data('SP_CLIENT_ID')
SP_CLIENT_SECRET = get_decrypted_data('SP_CLIENT_SECRET')

# Verificación de existencia de datos de conexión
if not all([DB_DBNAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, SP_CLIENT_ID, SP_CLIENT_SECRET]):
    logging.error("Faltan datos de conexión necesarios.")
    raise SystemExit("Deteniendo la ejecución debido a falta de datos de conexión.")
