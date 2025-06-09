import requests
import pandas as pd
import os
from dotenv import load_dotenv 

load_dotenv()

def fetch_and_save_stops_data():
    API_KEY = os.getenv("API_KEY")

    if not API_KEY:
        print("[❌] Błąd: Klucz API nie został znaleziony. Upewnij się, że plik .env istnieje i zawiera API_KEY.")
        return

    resource_id = "ab75c33d-3a26-4342-b36a-6e5fef0a3ac3"
    action = "dbstore_get"

    url = f"https://api.um.warszawa.pl/api/action/{action}/?id={resource_id}&apikey={API_KEY}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[❌] Błąd podczas zapytania HTTP: {e}")
        return

    try:
        json_data = response.json()
    except requests.exceptions.JSONDecodeError:
        print(f"[❌] Błąd dekodowania JSON. Odpowiedź nie jest poprawnym JSON-em.")
        print(f"Status Code: {response.status_code}")
        print(f"Raw Response Text: {response.text}")
        return

    if not isinstance(json_data.get("result"), list):
        print("[⚠️] API nie zwróciło poprawnej listy w kluczu 'result'.")
        return

    data = json_data["result"]

    if not data:
        print("[⚠️] Brak danych o przystankach w odpowiedzi API.")
        return

    flat_data = []
    for record in data:
        values = record.get("values", [])
        flat_record = {item["key"]: item["value"] for item in values}
        flat_data.append(flat_record)

    df = pd.DataFrame(flat_data)

    os.makedirs("data/raw/stops", exist_ok=True)
    filepath = os.path.join("data/raw/stops", "ztm_stops.parquet")
    df.to_parquet(filepath, index=False)
    print(f"[✅] Przystanki: Zapisano {len(df)} rekordów do {filepath}")