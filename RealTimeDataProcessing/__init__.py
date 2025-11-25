import sys
import os
from shared import constants
from shared import br_uncompress
import uuid
import azure.functions as func
import logging
import json
from datetime import datetime, timedelta
from scripts.air_RealTimeDataProcessing import Air_update

# Variables globales pour suivre les valeurs précédentes
previous_presentvalue = None
previous_timestamp = None
last_temperature = None
last_humidity = None
last_CO2 = None
last_COV = None


# all these decorators were commented to follow the old way of defining the function that has been implemented in f
# theses changes have been made to keep consistancy between function apps

def main(documents: func.DocumentList, outputDocument: func.Out[func.Document]):
    all_docs_to_insert = []

    if documents:
        for doc in documents:
            try:
                parsed = json.loads(doc.to_json())

                if parsed.get("values") is None:
                    parsed["values"] = {}

                # --------------------
                # Traitement Air
                # --------------------
                if "Air" in parsed.get("device", ""):
                    docs_from_air = Air_update(parsed, outputDocument)  # Air_update doit retourner la liste de documents
                    all_docs_to_insert.extend(docs_from_air)

                # --------------------
                # Traitement LT
                # --------------------
                elif "LT" in parsed.get("device", ""):
                    Consumption_update(parsed)
                    cleaned = {
                        "values": {
                            "presentvalue": parsed.get("values", {}).get("presentvalue"),
                            "Consumption": parsed.get("values", {}).get("Consumption"),
                            "Duration": parsed.get("values", {}).get("Duration")
                        },
                        "device": parsed.get("device"),
                        "deveui": parsed.get("deveui"),
                        "ReceivedTimeStamp": parsed.get("ReceivedTimeStamp"),
                        "RoundReceivedTimeStamp": round_timestamp(parsed.get("ReceivedTimeStamp")),
                        "metadata": parsed.get("metadata"),
                        "id": generate_id(parsed),
                        "_rid": parsed.get("_rid"),
                        "_self": parsed.get("_self"),
                        "_etag": parsed.get("_etag"),
                        "_attachments": parsed.get("_attachments"),
                        "_ts": parsed.get("_ts")
                    }
                    all_docs_to_insert.append(cleaned)

                # --------------------
                # Traitement Desk
                # --------------------
                elif "Desk" in parsed.get("device", ""):
                    Consumption_update(parsed)
                    cleaned = {
                        "values": {
                            "temperature": parsed.get("values", {}).get("temperature"),
                            "humidity": parsed.get("values", {}).get("humidity"),
                            "occupancy": parsed.get("values", {}).get("occupancy"),
                            "vdd": parsed.get("values", {}).get("vdd")
                        },
                        "device": parsed.get("device"),
                        "deveui": parsed.get("deveui"),
                        "ReceivedTimeStamp": parsed.get("ReceivedTimeStamp"),
                        "RoundReceivedTimeStamp": round_timestamp(parsed.get("ReceivedTimeStamp")),
                        "metadata": parsed.get("metadata"),
                        "id": generate_id(parsed),
                        "_rid": parsed.get("_rid"),
                        "_self": parsed.get("_self"),
                        "_etag": parsed.get("_etag"),
                        "_attachments": parsed.get("_attachments"),
                        "_ts": parsed.get("_ts")
                    }
                    all_docs_to_insert.append(cleaned)

                # --------------------
                # Traitement TempEx
                # --------------------
                elif "TempEx" in parsed.get("device", ""):
                    Consumption_update(parsed)
                    cleaned = {
                        "values": {
                            "humidity": parsed.get("values", {}).get("humidity"),
                            "temperature": parsed.get("values", {}).get("temperature"),
                        },
                        "device": parsed.get("device"),
                        "deveui": parsed.get("deveui"),
                        "ReceivedTimeStamp": parsed.get("ReceivedTimeStamp"),
                        "RoundReceivedTimeStamp": round_timestamp(parsed.get("ReceivedTimeStamp")),
                        "metadata": parsed.get("metadata"),
                        "id": generate_id(parsed),
                        "_rid": parsed.get("_rid"),
                        "_self": parsed.get("_self"),
                        "_etag": parsed.get("_etag"),
                        "_attachments": parsed.get("_attachments"),
                        "_ts": parsed.get("_ts")
                    }
                    all_docs_to_insert.append(cleaned)

                # --------------------
                # Traitement Light
                # --------------------
                elif "Light" in parsed.get("device", ""):
                    Consumption_update(parsed)
                    cleaned = {
                        "values": {
                            "battery": parsed.get("values", {}).get("battery"),
                            "light_status": parsed.get("values", {}).get("light_status"),
                            "pir_status": parsed.get("values", {}).get("pir_status"),
                        },
                        "device": parsed.get("device"),
                        "deveui": parsed.get("deveui"),
                        "ReceivedTimeStamp": parsed.get("ReceivedTimeStamp"),
                        "RoundReceivedTimeStamp": round_timestamp(parsed.get("ReceivedTimeStamp")),
                        "metadata": parsed.get("metadata"),
                        "id": generate_id(parsed),
                        "_rid": parsed.get("_rid"),
                        "_self": parsed.get("_self"),
                        "_etag": parsed.get("_etag"),
                        "_attachments": parsed.get("_attachments"),
                        "_ts": parsed.get("_ts")
                    }
                    all_docs_to_insert.append(cleaned)
                # --------------------
                # Traitement Room
                # --------------------
                elif "Room" in parsed.get("device", ""):
                    Consumption_update(parsed)
                    cleaned = {
                        "values": {
                            "people_count_all": parsed.get("values", {}).get("people_count_all"),
                            "people_count_max": parsed.get("values", {}).get("people_count_max"),
                            "region_count": parsed.get("values", {}).get("region_count"),
                        },
                        "device": parsed.get("device"),
                        "deveui": parsed.get("deveui"),
                        "ReceivedTimeStamp": parsed.get("ReceivedTimeStamp"),
                        "RoundReceivedTimeStamp": round_timestamp(parsed.get("ReceivedTimeStamp")),
                        "metadata": parsed.get("metadata"),
                        "id": generate_id(parsed),
                        "_rid": parsed.get("_rid"),
                        "_self": parsed.get("_self"),
                        "_etag": parsed.get("_etag"),
                        "_attachments": parsed.get("_attachments"),
                        "_ts": parsed.get("_ts")
                    }
                    all_docs_to_insert.append(cleaned)
                # --------------------
                # Traitement Son
                # --------------------
                elif "Son" in parsed.get("device", ""):
                    Consumption_update(parsed)
                    cleaned = {
                        "values": {
                            "lmax": parsed.get("values", {}).get("lmax"),
                            "leq": parsed.get("values", {}).get("leq"),
                            "spl": parsed.get("values", {}).get("spl"),
                            "weighting_mode": parsed.get("values", {}).get("weighting_mode"),
                            "battery": parsed.get("values", {}).get("battery"),
                        },
                        "device": parsed.get("device"),
                        "deveui": parsed.get("deveui"),
                        "ReceivedTimeStamp": parsed.get("ReceivedTimeStamp"),
                        "RoundReceivedTimeStamp": round_timestamp(parsed.get("ReceivedTimeStamp")),
                        "metadata": parsed.get("metadata"),
                        "id": generate_id(parsed),
                        "_rid": parsed.get("_rid"),
                        "_self": parsed.get("_self"),
                        "_etag": parsed.get("_etag"),
                        "_attachments": parsed.get("_attachments"),
                        "_ts": parsed.get("_ts")
                    }
                    all_docs_to_insert.append(cleaned)

            except Exception as e:
                logging.error(f"Error: {e}")

    # --------------------
    # Une seule insertion pour tout le batch
    # --------------------
    if all_docs_to_insert:
        outputDocument.set([func.Document.from_dict(d) for d in all_docs_to_insert])
        logging.info(f"{len(all_docs_to_insert)} documents insérés dans RealTimeDataProcessing.")
        
def Consumption_update(item_to_insert):
    """ Fonction spécifique au capteur LT_xx-xx : calcul des champs 'Consumption' et 'Duration'. """
    global previous_presentvalue, previous_timestamp

    # Vérifie que le nom du capteur commence par "LT"
    device_name = item_to_insert.get("device", "")
    if not device_name.startswith("LT"):
        return  # On sort de la fonction sans rien faire

    current_presentvalue = item_to_insert['values'].get('presentvalue')
    if current_presentvalue is not None:
        if previous_presentvalue is not None:
            consumption = current_presentvalue - previous_presentvalue
        else:
            consumption = 0  # Pas de consommation pour le premier enregistrement

        item_to_insert['values']['Consumption'] = consumption
        previous_presentvalue = current_presentvalue

        timestamp = datetime.fromisoformat(item_to_insert['ReceivedTimeStamp'].replace('Z', '+00:00'))
        if previous_timestamp is not None:
            duration = round((timestamp - previous_timestamp).total_seconds())
        else:
            duration = 0  # Pas de durée pour le premier enregistrement

        item_to_insert['values']['Duration'] = duration
        previous_timestamp = timestamp

        # Réorganiser les clés pour placer 'Consumption' et 'Duration' après 'presentvalue'
        values = item_to_insert['values']
        reordered_values = {
            "presentvalue": values["presentvalue"],
            "Consumption": values["Consumption"],
            "Duration": values["Duration"]
        }
        item_to_insert['values'] = reordered_values
    else:
        print(f"Clé 'presentvalue' manquante dans l'élément : {item_to_insert}")
        
def replace_nulls_with_last_value(document):
    """ Fonction pour remplacer les valeurs nulles par les dernières valeurs connues. """
    global last_temperature, last_humidity, last_CO2, last_COV
    
    # Vérification si "values" existe avant d'y accéder
    values = document.get("values", {})
    
    if values.get("temperature") is None:
        values["temperature"] = last_temperature if last_temperature is not None else 0
    else:
        last_temperature = values["temperature"]
    
    if values.get("humidity") is None:
        values["humidity"] = last_humidity if last_humidity is not None else 0
    else:
        last_humidity = values["humidity"]
    
    if values.get("CO2") is None:
        values["CO2"] = last_CO2 if last_CO2 is not None else 0
    else:
        last_CO2 = values["CO2"]
    
    if values.get("COV") is None:
        values["COV"] = last_COV if last_COV is not None else 0
    else:
        last_COV = values["COV"]
    
    return document

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

def generate_id(parsed):
    # Générer un nouvel ID basé sur une combinaison des informations
    return str(uuid.uuid4())
