from src.fetch_gps import fetch_and_save_gps_data
from src.fetch_stops import fetch_and_save_stops_data
from dotenv import load_dotenv 
load_dotenv() 

def main():
    print("🔄 Rozpoczynam pobieranie danych GPS...")
    fetch_and_save_gps_data()

    print("🗺️ Rozpoczynam pobieranie danych o przystankach...")
    fetch_and_save_stops_data()

    print("✅ Zakończono pobieranie danych.")


if __name__ == "__main__":
    main()