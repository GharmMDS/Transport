import requests
import pandas as pd
import os
from datetime import datetime

def fetch_and_save_gps_data():
    api_key = "565e7557-c6d8-4b22-9aef-03af20ab4102"
    url = f"https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id=f2e5503e927d-4ad3-9500-4ab9e55deb59&apikey={api_key}&type=1"

    save_dir = "data/raw/gps"
    os.makedirs(save_dir, exist_ok=True)

    response = requests.get(url)
    data = response.json().get("result", [])

    if data:
        df = pd.DataFrame(data)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filepath = os.path.join(save_dir, f"ztm_gps_{timestamp}.parquet")
        df.to_parquet(filepath, index=False)
        print(f"[✅] GPS: Zapisano {len(df)} rekordów do {filepath}")
    else:
        print("[⚠️] GPS: Brak danych do zapisania.")
