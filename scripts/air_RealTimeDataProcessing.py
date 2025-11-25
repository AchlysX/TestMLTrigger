import os
import sys
import subprocess
from typing import OrderedDict
import uuid
import azure.functions as func
import logging
import json
from datetime import datetime, timedelta, timezone
from azure.cosmos import CosmosClient

# -------------------------
# Configuration du chemin 
# -------------------------

# Dossier courant = scripts
current_dir = os.path.dirname(os.path.abspath(__file__))

# Dossier parent = DataProcessingSmartBuilding
project_root = os.path.abspath(os.path.join(current_dir, '..'))

# Dossier shared
shared_dir = os.path.join(project_root, 'shared')

# Ajouter shared au sys.path si nécessaire pour l'import
if shared_dir not in sys.path:
    sys.path.insert(0, shared_dir)

# Import depuis shared
from shared import br_uncompress
from shared import constants

logging.info(f"[DEBUG] br_uncompress chargé depuis : {os.path.abspath(br_uncompress.__file__)}")

# Chemin absolu vers le script de décodage pour subprocess
DECODER_PATH = os.path.join(shared_dir, 'br_uncompress.py')
if not os.path.isfile(DECODER_PATH):
    logging.error(f"Le script de décodage n'existe pas : {DECODER_PATH}")
else:
    logging.info(f"[DEBUG] Le script br_uncompress existe : {DECODER_PATH}")

# -------------------------
# Variables globales
# -------------------------
previous_presentvalue = None
previous_timestamp = None
last_temperature = None
last_humidity = None
last_CO2 = None
last_COV = None

# CosmosDB
COSMOS_CONN_STRING = os.environ.get("cosmosdbchateaudunparis_DOCUMENTDB")
DATABASE_NAME = "Smartoffice"
CONTAINER_NAME = "Sensordata"

client = CosmosClient.from_connection_string(COSMOS_CONN_STRING)
database = client.get_database_client(DATABASE_NAME)
cosmos_container = database.get_container_client(CONTAINER_NAME)

# Conteneur de destination pour les données nettoyées et décodées
destination_container = database.get_container_client('RealTimeDataProcessing')
# -------------------------
# Fonctions utilitaires
# -------------------------

def round_timestamp(timestamp_str):
    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    minute = timestamp.minute
    remainder = minute % 10

    if remainder >= 5:
        rounded_minute = minute + (10 - remainder)
    else:
        rounded_minute = minute - remainder

    # Si on dépasse 59 minutes, on avance d'une heure
    if rounded_minute == 60:
        timestamp = timestamp.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        timestamp = timestamp.replace(minute=rounded_minute, second=0, microsecond=0)

    return timestamp.isoformat().replace('+00:00', 'Z')


def get_latest_values_before(timestamp_iso):
    """Récupère les dernières valeurs non nulles de température, humidité, CO2 et COV avant le timestamp donné."""
    query = f"""
    SELECT TOP 1 c.values.temperature, c.values.humidity, c.values.CO2, c.values.COV
    FROM c
    WHERE c.device = 'Air_05-01' AND c.ReceivedTimeStamp < '{timestamp_iso}'
    ORDER BY c.ReceivedTimeStamp DESC
    """
    items = list(cosmos_container.query_items(query=query, enable_cross_partition_query=True))
    return items[0] if items else {}


def replace_nulls_with_last_value(document):
    """Remplace les valeurs nulles par la dernière valeur connue."""
    global last_temperature, last_humidity, last_CO2, last_COV

    if not isinstance(document.get("values"), dict):
        document["values"] = {}

    fallback = get_latest_values_before(document["ReceivedTimeStamp"])

    for key, last_value in [('temperature', last_temperature), ('humidity', last_humidity),
                            ('CO2', last_CO2), ('COV', last_COV)]:
        val = document["values"].get(key)
        if val is None:
            document["values"][key] = last_value or fallback.get(key)
        else:
            if key == 'temperature':
                last_temperature = val
            elif key == 'humidity':
                last_humidity = val
            elif key == 'CO2':
                last_CO2 = val
            elif key == 'COV':
                last_COV = val

    return document


def remove_data(item, field_list):
    """Supprime des champs dans le dictionnaire passé en paramètre."""
    for field in field_list:
        item.pop(field, None)


# -------------------------
# Décodage du capteur Air
# -------------------------

def decode_frame(raw_data, timestamp):
    """Décoder les trames du capteur Air en appelant le script br_uncompress.py."""
    if not os.path.isfile(DECODER_PATH):
        logging.error(f"Le script de décodage n'existe pas : {DECODER_PATH}")
        return None

    command = [
        "python",
        "-m",
        "shared.br_uncompress",  # appel en tant que module
        "-a",
        "-t", timestamp,
        "3",
        "1,10,7,temperature",
        "2,100,6,humidity",
        "3,10,6,CO2",
        "4,10,6,COV",
        "-if",
        raw_data
    ]

    try:
        logging.info(f"Commande exécutée : {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        output = result.stdout.strip()

        if not output:
            logging.warning("Le script de décodage n'a renvoyé aucun résultat.")
            return None

        logging.info(f"Sortie du script de décodage : {output}")
        return json.loads(output)

    except subprocess.CalledProcessError as e:
        logging.error(f"Erreur lors du décodage : {e}")
        logging.error(f"Erreur de la commande : {e.stderr.strip()}")
        return None

    except json.JSONDecodeError as e:
        logging.error(f"Erreur de décodage JSON : {e}")
        return None


# -------------------------
# Mise à jour du capteur Air
# -------------------------

def Air_update(item, outputDocument):
    """Traitement spécifique au capteur Air."""
    raw_data = item.get('raw')
    received_timestamp = item.get('ReceivedTimeStamp')

    if not raw_data or not received_timestamp:
        logging.warning("Données manquantes pour le traitement Air_update.")
        return

    logging.info(f"Raw data envoyé au script : {raw_data}")
    trimmed_timestamp = received_timestamp[:23] + "Z"

    decoded_result = decode_frame(raw_data, trimmed_timestamp)
    if not decoded_result:
        logging.warning("Aucun résultat décodé. Aucun document n'a été inséré.")
        return

    grouped_data = {}
    for data_point in decoded_result.get('dataset', []):
        remove_data(data_point, ['data_relative_timestamp'])
        round_received_timestamp = round_timestamp(data_point['data_absolute_timestamp'])

        if round_received_timestamp not in grouped_data:
            grouped_data[round_received_timestamp] = {
                "values": {},
                "device": "Air_05-01",
                "deveui": "70B3D5E75E01C1FB",
                "ReceivedTimeStamp": data_point['data_absolute_timestamp'],
                "RoundReceivedTimeStamp": round_received_timestamp,
                "metadata": item.get('metadata', {})
            }

        label_name = data_point['data']['label_name']
        value = data_point['data']['value']

        # Conversion
        if label_name in ['temperature', 'humidity']:
            value = round(value * 0.01, 1)

        grouped_data[round_received_timestamp]["values"][label_name] = value

    documents_to_insert = []
    for key, value in grouped_data.items():
        ordered_values = OrderedDict([
            ('temperature', value['values'].get('temperature')),
            ('humidity', value['values'].get('humidity')),
            ('CO2', value['values'].get('CO2')),
            ('COV', value['values'].get('COV')),
        ])
        value['values'] = ordered_values
        item_to_insert = replace_nulls_with_last_value(value)
        item_to_insert['id'] = str(uuid.uuid4())
        documents_to_insert.append(item_to_insert)
        logging.info(f"Document préparé pour insertion : {json.dumps(item_to_insert, indent=4)}")
        logging.info(f"{len(documents_to_insert)} documents insérés.")
    return documents_to_insert

# -------------------------
# Fonction principale
# -------------------------

def main(documents: func.DocumentList, outputDocument: func.Out[func.Document]):
    if documents:
        for doc in documents:
            try:
                if not doc.to_json():
                    logging.warning("Le document est vide ou mal formaté.")
                    return

                logging.info(f"Document brut : {doc.to_json()}")
                parsed = json.loads(doc.to_json())

                if parsed.get("values") is None:
                    parsed["values"] = {}

                Air_update(parsed, outputDocument)
                logging.info(f"Traitement terminé pour le document : {parsed}")

            except Exception as e:
                logging.error(f"Error: {e}")
