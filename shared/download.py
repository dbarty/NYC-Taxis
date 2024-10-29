import requests
import os
from bs4 import BeautifulSoup

class Downloader:
    DATAURL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    
    def __init__(self, datadir="data/"):
        self.datadir = datadir
        self.rawdatadir = datadir + "raw/"
        os.makedirs(os.path.dirname(self.datadir), exist_ok=True)
        os.makedirs(os.path.dirname(self.rawdatadir), exist_ok=True)

    
    def copy_all(self):
        "Copy all data files from www.nyc.gov website"
        self.copy_all_tripdata()
        self.copy_taxi_zones()

    
    def copy_all_tripdata(self, reload:bool=False) -> None:
        "Copy all tripdata files from www.nyc.gov website"
        response = requests.get(self.DATAURL)

        if 200 != response.status_code:
            return
            
        soup = BeautifulSoup(response.text, features="lxml")
        links = soup.find_all("a")

        for link in links:
            # skip non .parquet links
            if not link["href"].endswith(".parquet"):
                continue

            self.copy_tripdata(link["href"], reload)

    
    def copy_tripdata(self, url:str, reload:bool=False) -> None:
        "Copy single tripdata file from www.nyc.gov website"
        datafile = self.rawdatadir + self.extract_filename(url)
        self._copy_url_to_file(url, datafile, reload)


    def copy_taxi_zones(self, reload:bool=False) -> None:
        "Copy taxi_zones_lookup.csv file from www.nyc.gov website"
        url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
        datafile = self.datadir + self.extract_filename(url)
        self._copy_url_to_file(url, datafile, reload)


    def extract_filename(self, path:str) -> str:
        "Extract the filename from a path"
        return path.split("/")[-1:].pop(0)

    
    def _copy_url_to_file(self, url:str, file:str, reload:bool=False) -> None:
        # skip if file already exists
        if not reload and os.path.exists(file):
            print(f"Skip download, because file exists: {file}")
            return

        # Copy content from url into file
        with open(file, "wb") as fp:
            print(f"Download file: {file}")
            response = requests.get(url)
            fp.write(response.content)

