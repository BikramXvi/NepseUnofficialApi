import os
import requests
import pandas as pd
from datetime import datetime
import time


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
    r"A:\NepseCSV",                
    os.path.join(os.getcwd(), "to-csv")  
]


SCRIPS = [
    "USHL","ACLBSL","ACLBSLP","ANLB","ANLBP","ADBL","ADBLD83","ADBLB","ADBLB86","ADBLB87",
    "AKJCL","API","AKPL","AHPC","AVU","ALBSL","ALBSLP","AHL","ALICLP","ALICL","AVYAN","AVYANP",
    "BHL","BOKD86","BOKD86KA","BHPL","BARUN","BFC","BFCPO","BGWT","BEDC","BHCL","BHDC","BSL",
    "BBC","BNL","BNT","BNHC","BPCL","BSM","CMB","CMF2","CHDC","CFCL","CFCLPO","CCBD88","CGH",
    "CBBLPO","CBBL","CHL","CHCL","CKHL","CIT","CITPO","CLI","CLIP","C30MF","CIZBD90","CZBILP",
    "CZBIL","CIZBD86","CITY","CBLD88","PSDBLP","CORBL","CORBLP","CREST","CRESTP","CFL","CYCL",
    "CYCLP","DDBL","DDBLPO","DLBS","DHPL","DOLTI","DORDI","EHPL","ENL","EBL","EBLPO","EBLD85",
    "EBLEB89","EBLD86","EBLD91","EDBLPO","EDBL","FMDBL","FMDBLP","FHL","FOWAD","FOWADP","GMFBS",
    "GMFBSP","GBBL","GBBLPO","GSY","GBBD85","GHL","GCIL","GIBF1","GBIME","GBIMEP","GBIMESY2",
    "GBILD86/87","GBILD84/85","GILBPO","GILB","GFCL","GFCLPO","GWFD83","GRU","GBLBS","PDBLPO",
    "GBLBSP","GRDBL","GRDBLP","GVL","GLH","GMLI","GMLIP","GMFIL","GMFILP","GLBSL","GUFL","GUFLPO",
    "HBT","HATHY","HDHPC","HURJA","HBLD86","H8020","HBLD83","HBL","HBLPO","HDL","HEI","HEIP","HFL",
    "HHL","HLBSL","HLBSLP","HLI","HLIPO","HPPL","HRL","HRLP","HIMSTAR","HIDCL","HIDCLP","ILI","ILIP",
    "ICFC","ICFCPO","ICFCD83","ICFCD88","IGIPO","IGI","ILBS","ILBSP","IHL","JFL","JFLPO","JSLBB",
    "JSLBBP","JBLBP","JBLB","JOSHI","JBBD87","JBBLPO","JBBL","JSM","KMCDB","KMCDBP","KPCL","KDL",
    "KDLP","KSBBLP","KSBBL","KSBBLD87","KRBL","KRBLPO","KKHC","KEF","KBLD86","KBLPO","KBL","KBLD89",
    "KDBY","KBLD90","KSY","KBSH","LUK","LLBSPO","LLBS","LBLD88","LVF2","LSL","LSLPO","SFMF","LBLD86",
    "LEC","LICN","LICNPO","LBBLD89","LBBL","LBBLPO","MBLD2085","MBLPO","MBL","MBLEF","MBLD87","MBJC",
    "MLBLD89","MLBL","MLBLPO","MDBLPO","MLBSL","MLBSLP","MSLB","MSLBP","MKHL","MKJC","MAKAR","MEHL",
    "MHL","MANDU","MFILPO","MFLD85","MFIL","MLBS","MLBSP","MMKJL","MATRIP","MATRI","MKHC","MMF1",
    "MCHL","MERO","MEROPO","MSHL","MDB","MDBPO","MLBBLP","MLBBL","MEL","MHCL","MEN","MHNL","MND84/85",
    "MNMF1","MNBBL","MNBBLP","MKCL","MPFL","MPFLPO","NABIL","NABILP","NBF2","NBLD82","NBF3","NBLD85",
    "NABILD87","NADEP","NADEPP","NABBCP","NABBC","NMFBS","NMFBSP","NHPC","NLICLP","NLICL","NIL","NILPO",
    "NBBD2085","NBL","NBLP","NBLD87","NBBU","NCCD86","NTC","NFD","NFSPO","NFS","NHDL","NIFRA","NIFRAP",
    "NIFRAGED","NIFRAUR85/86","NICL","NICLPO","NIMBD90","NIBLSTF","NIBSF2","NIBD84","NIBD2082","NIMB",
    "NIMBPO","NKU","NLIC","NLICP","NLO","NMIC","NMICP","NRIC","NRICP","NRM","SBI","SBIPO","SBIBD86",
    "SBID89","SBID83","NSM","NSMPO","NTL","NVG","NWCL","NWC","NMLBBLP","NMLBBL","NESDO","NESDOP",
    "NGPL","NIBLGF","NICD88","NICFC","NICAD2091","NICGF2","NICA","NICAP","NICD83/84","NICAD8283",
    "NICAD85/86","NICBF","NICSF","NICLBSL","NICLBSLP","NUBL","NUBLPO","NLGPO","NLG","NMBMF","NMBMFP",
    "NMBPO","NMB","NMBD2085","NMBHF2","NMBUR93/94","NMBD87/88","NMBEB92/93","NMBD89/90","NSIF2","NMB50",
    "NRN","NYADI","OMPL","OHL","PMHPL","PPCL","PHCL","PPL","PFL","PFLPO","PRVU","PRFLPO","PRVUPO",
    "PBLD86","PBLD84","PBLD87","PSF","PRSF","PRINPO","PRIN","PMLI","PMLIP","PCBLP","PCBL","PBD84",
    "PBD85","PBD88","PROFL","PROFLP","PURE","RADHI","RJM","RHGCL","RBBD83","RBBD2088","RBCL","RBCLPO",
    "RHPL","RAWA","RMF1","RMF2","RSY","RNLI","RNLIP","RLFLPO","RLFL","RIDI","RFPL","RSDC","RSDCP",
    "RURU","SABSLPO","SMJC","SALICO","SALICOPO","SAHAS","STC","SAMAJ","SAMAJP","SMATA","SMATAP","SFC",
    "SPC","SMPDA","SMPDAP","SFCL","SFCLP","SLBSL","SLBSLPO","SKBBL","SKBBLP","SNMAPO","SANIMA","SAND2085",
    "SBD87","SLCF","SBD89","SGIC","SGICP","SAGF","SHPC","TAMOR","SRLI","SRLIP","SJCL","SANVI","SAPDBL",
    "SAPDBLP","SARBTM","SPHL","SADBLP","SADBL","SDBD87","SICL","SICLPO","SHINEP","SHINE","SSHL","SHIVM",
    "SBPP","SIFC","SIFCPO","SRS","SHLB","SHLBP","SPL","SBLD89","SIGS3","SBL","SEOS","SBLPO","SBLD83",
    "SBLD84","SBLD2082","SIGS2","SEF","SPIL","SPILPO","SIKLES","SINDU","SINDUP","SHEL","SHL","SONA",
    "SCB","SCBPO","SCBD","SNLI","SNLIP","SRBLD83","SBCF","SFEF","SMHL","SMH","SMB","SMBPO","SJLICP",
    "SJLIC","SWMF","SWMFPO","SWBBL","SWBBLP","SMFBS","SMFBSP","SLBBL","SLBBLP","SGHC","SPDL","TRH",
    "TPC","TSHL","TTL","TVCL","UNL","UNHPL","UNLB","UNLBP","UAILPO","UAIL","UMRH","UMHL","UPCL",
    "USLB","USLBP","ULBSL","ULBSLPO","UHEWA","ULHC","USHEC","UPPER","VLBS","VLBSPO","VLUCL","WNLB",
    "WNLBP","YHL"
]



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
    print(f"OK Saved {filename} to {folder_name}")

def process_data(data, endpoint_name, extra_cols=None):
    if extra_cols is None:
        extra_cols = {}

    
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

    
    elif isinstance(data, dict):
        df = pd.json_normalize([data], sep="_")
        for k, v in extra_cols.items():
            df[k] = v
        df["fetched_at"] = datetime.now()
        df["date"] = datetime.now().strftime("%Y-%m-%d")
        df["source_endpoint"] = endpoint_name

    
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
            rows.append(row)  
        df = pd.DataFrame(rows)

    
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
            row.update(extra_cols)  
            rows.append(row)
        df = pd.DataFrame(rows)

    
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


def main():
    while True:
        start_time = datetime.now()
        print(f"=== Fetch started at {start_time} ===")

        
        for url_list, folder_name in [(NEPSEAPI_URLS, "NepseAPI"), 
                                      (OTHER_API_URLS, "OtherAPI")]:
            for url in url_list:
                data = fetch_json(url)
                if data:
                    endpoint_name = url.split("/")[-1]
                    df = process_data(data, endpoint_name)
                    if df is not None and not df.empty:
                        filename = f"{endpoint_name}.csv"   
                        save_df_to_all_folders(df, folder_name, filename)
                    else:
                        print(f"Data empty for {url}")
                else:
                    print(f"Failed to fetch {url}")

        
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
                print(f"No data for symbol {sym}")

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            filename = "DailyScripPriceGraph.csv"
            save_df_to_all_folders(combined_df, os.path.join("GraphAPI", "DailyScripPriceGraph"), filename)

        end_time = datetime.now()
        print(f"=== Fetch finished at {end_time} ===")

        
        time.sleep(900)

if __name__ == "__main__":
    main()


