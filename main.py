import os
import requests
import pandas as pd
from datetime import datetime
import time

# ----------------- CONFIG -----------------
BASE_URL = "http://127.0.0.1:8000"

NEPSEAPI_URLS = [
    f"{BASE_URL}/PriceVolume",
    f"{BASE_URL}/Summary",
    f"{BASE_URL}/SupplyDemand",
    f"{BASE_URL}/TopGainers",
    f"{BASE_URL}/TopLosers",
    f"{BASE_URL}/TopTenTradeScrips",
    f"{BASE_URL}/TopTenTurnoverScrips",
    f"{BASE_URL}/TopTenTransactionScrips",
    f"{BASE_URL}/IsNepseOpen",
    f"{BASE_URL}/NepseIndex",
    f"{BASE_URL}/NepseSubIndices",
]

GRAPH_API_URLS = [
    f"{BASE_URL}/DailyNepseIndexGraph"
]

OTHER_API_URLS = [
    f"{BASE_URL}/CompanyList",
    f"{BASE_URL}/SecurityList",
    f"{BASE_URL}/TradeTurnoverTransactionSubindices",
    f"{BASE_URL}/LiveMarket",
    f"{BASE_URL}/MarketDepth",
]

FOLDERS = [
    r"A:\NepseCSV",                # Save to A: drive
    os.path.join(os.getcwd(), "to-csv")  # Save locally
]

# ----------------- SCRIPTS -----------------
SCRIPS = [
    "USHL","ACLBSL","ACLBSLP","ANLB","ANLBP","ADBL","ADBLD83","ADBLB",
    # trimmed here for brevity — keep full list in your script
    "WNLB","WNLBP","YHL"
]

# ----------------- HELPERS -----------------
def fetch_json(url):
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ Error fetching {url}: {e}")
        return None

def save_df_to_all_folders(df, folder_name, filename):
    for base in FOLDERS:
        folder_path = os.path.join(base, folder_name)
        os.makedirs(folder_path, exist_ok=True)
        filepath = os.path.join(folder_path, filename)
        df.to_csv(filepath, mode="w", header=True, index=False)
    print(f"✅ Saved {filename} to {folder_name}")

def process_data(data, endpoint_name, extra_cols=None):
    if extra_cols is None:
        extra_cols = {}

    # ----- TradeTurnoverTransactionSubindices -----
    if "tradeturnovertransactionsubindices" in endpoint_name.lower():
        rows = []
        for scrip, sdata in data.get("scripsDetails", {}).items():
            row = sdata.copy()
            row.update(extra_cols)
            row["fetched_at"] = datetime.now()
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row["source_endpoint"] = endpoint_name
            rows.append(row)
        df = pd.DataFrame(rows)

    # ----- NepseIndex / NepseSubIndices -----
    elif isinstance(data, dict) and ("nepseindex" in endpoint_name.lower() or "nepsesubindices" in endpoint_name.lower()):
        rows = []
        for key, value in data.items():
            row = value.copy()
            row.update(extra_cols)
            row["fetched_at"] = datetime.now()
            row["date"] = datetime.now().strftime("%Y-%m-%d")
            row["source_endpoint"] = endpoint_name
            rows.append(row)
        df = pd.DataFrame(rows)

    # ----- Generic dict -----
    elif isinstance(data, dict):
        df = pd.json_normalize([data], sep="_")
        for k, v in extra_cols.items():
            df[k] = v
        df["fetched_at"] = datetime.now()
        df["date"] = datetime.now().strftime("%Y-%m-%d")
        df["source_endpoint"] = endpoint_name

    # ----- DailyNepseIndexGraph -----
    elif isinstance(data, list) and endpoint_name.lower() == "dailynepseindexgraph":
        rows = []
        for item in data:
            ts, index_value = item
            row = {
                "timestamp": ts,
                "datetime": datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"),
                "index_value": index_value,
                "fetched_at": datetime.now(),
                "date": datetime.now().strftime("%Y-%m-%d"),
                "source_endpoint": endpoint_name,
            }
            rows.append(row)  # ✅ fixed indentation
        df = pd.DataFrame(rows)

    # ----- DailyScripPriceGraph -----
    elif isinstance(data, list) and endpoint_name.lower() == "dailyscrippricegraph":
        rows = []
        for item in data:
            ts, price = item
            row = {
                "timestamp": ts,
                "datetime": datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "fetched_at": datetime.now(),
                "date": datetime.now().strftime("%Y-%m-%d"),
                "source_endpoint": endpoint_name,
            }
            row.update(extra_cols)  # symbol included here
            rows.append(row)
        df = pd.DataFrame(rows)

    # ----- Generic list -----
    elif isinstance(data, list):
        df = pd.json_normalize(data, sep="_")
        for k, v in extra_cols.items():
            df[k] = v
        df["fetched_at"] = datetime.now()
        df["date"] = datetime.now().strftime("%Y-%m-%d")
        df["source_endpoint"] = endpoint_name

    else:
        return None

    df.fillna(0, inplace=True)
    return df

# ----------------- MAIN -----------------
def main():
    while True:
        start_time = datetime.now()
        print(f"=== Fetch started at {start_time} ===")

        # ---- Regular API endpoints ----
        for url_list, folder_name in [(NEPSEAPI_URLS, "NepseAPI"), 
                                      (OTHER_API_URLS, "OtherAPI")]:
            for url in url_list:
                data = fetch_json(url)
                if data:
                    endpoint_name = url.split("/")[-1]
                    df = process_data(data, endpoint_name)
                    if df is not None and not df.empty:
                        filename = f"{endpoint_name}.csv"   # overwrite
                        save_df_to_all_folders(df, folder_name, filename)
                    else:
                        print(f"❌ Data empty for {url}")
                else:
                    print(f"❌ Failed to fetch {url}")

        # ---- Graph APIs ----
        nepse_graph = fetch_json(f"{BASE_URL}/DailyNepseIndexGraph")
        if nepse_graph:
            df = process_data(nepse_graph, "DailyNepseIndexGraph")
            if df is not None and not df.empty:
                filename = "DailyNepseIndexGraph.csv"
                save_df_to_all_folders(df, "GraphAPI", filename)

        all_data = []
        for sym in SCRIPS:
            sym_url = f"{BASE_URL}/DailyScripPriceGraph/{sym}"
            sym_data = fetch_json(sym_url)
            if sym_data:
                df = process_data(sym_data, "DailyScripPriceGraph", extra_cols={"symbol": sym})
                if df is not None and not df.empty:
                    all_data.append(df)
            else:
                print(f"❌ No data for symbol {sym}")

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            filename = "DailyScripPriceGraph.csv"
            save_df_to_all_folders(combined_df, os.path.join("GraphAPI", "DailyScripPriceGraph"), filename)

        end_time = datetime.now()
        print(f"=== Fetch finished at {end_time} ===")

        # Wait 15 minutes before next fetch
        time.sleep(900)

if __name__ == "__main__":
    main()
