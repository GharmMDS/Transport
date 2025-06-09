import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv 

load_dotenv()

def fetch_and_save_gps_data():
    api_key = os.getenv("API_KEY")

    if not api_key:
        print("[❌] Błąd: Klucz API nie został znaleziony. Upewnij się, że plik .env istnieje i zawiera API_KEY.")
        return
    # "type=1" to autobusy, "type=2" to tramwaje
    resource_id = "f2e5503e927d-4ad3-9500-4ab9e55deb59"
    vehicle_type = 1 

    url = f"https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id={resource_id}&apikey={api_key}&type={vehicle_type}"

    save_dir = "data/raw/gps"
    os.makedirs(save_dir, exist_ok=True)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[❌] Błąd podczas zapytania HTTP dla GPS: {e}")
        return

    try:
        json_data = response.json()
    except requests.exceptions.JSONDecodeError:
        print(f"[❌] Błąd dekodowania JSON dla GPS. Odpowiedź nie jest poprawnym JSON-em.")
        print(f"Status Code: {response.status_code}")
        print(f"Raw Response Text: {response.text}")
        return

    data = json_data.get("result", [])

    if not data:
        print("[⚠️] GPS: Brak danych do zapisania.")
        return

    df = pd.DataFrame(data)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filepath = os.path.join(save_dir, f"ztm_gps_{timestamp}.parquet")
    df.to_parquet(filepath, index=False)
    print(f"[✅] GPS: Zapisano {len(df)} rekordów do {filepath}")