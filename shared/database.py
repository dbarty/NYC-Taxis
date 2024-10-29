import os
import pandas as pd
import sqlite3
from shared.processor import TripdataProcessor

TABLES = {
    "trips": """CREATE TABLE IF NOT EXISTS trips (
        vendor_id INTEGER NOT NULL,
        vendor_name TEXT,
        pickup_at timestamp,
        dropoff_at timestamp,
        pickup_location_id INTEGER,
        dropoff_location_id INTEGER,
        store_and_fwd_flag TEXT,
        ratecode_id INTEGER,
        passenger_count INTEGER,
        trip_distance_mi REAL,
        extra REAL,
        mta_tax REAL,
        fare_amount REAL,
        tolls_amount REAL,
        tip_amount REAL,
        total_amount REAL,
        payment_type TEXT,
        trip_type INTEGER,
        trip_category TEXT,
        improvement_surcharge REAL,
        congestion_surcharge REAL,
        ehail_fee REAL,
        airport_fee REAL
    )""",
    "zone_lockups": """CREATE TABLE IF NOT EXISTS zone_lookups (
        location_id INTEGER NOT NULL,
        borough TEXT,
        zone TEXT,
        service_zone TEXT
    )""",
    "imports": """CREATE TABLE IF NOT EXISTS imports (
        imported_at timestamp,
        url TEXT,
        file TEXT,
        total_lines INTEGER
    )"""
}

class Database:
    def __init__(self, name='taxis', datadir='data/'):
        self.datadir = datadir
        self.dbdatadir = datadir + "db/"
        self.rawdatadir = datadir + "raw/"
        os.makedirs(os.path.dirname(self.datadir), exist_ok=True)
        os.makedirs(os.path.dirname(self.dbdatadir), exist_ok=True)
        os.makedirs(os.path.dirname(self.rawdatadir), exist_ok=True)
        self._name = self.dbdatadir + name + '.db' if name != ':memory:' else name
        self._con = None

    
    def __enter__(self):
        print("Open Database")
        setup = not os.path.exists(self._name) or self._name == ':memory:'
        self._con = sqlite3.connect(self._name)
        self._cursor = self._con.cursor()

        if setup:
            self._setup()

        return self

    
    def __exit__(self, exc_type, exc_value, traceback):
        print("Close Database")
        self._con.close()


    @property
    def tables(self) -> list:
        raw_tables = self._cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        return [table[0] for table in raw_tables]

    def import_all_tripdata(self) -> None:
        files = os.listdir(self.rawdatadir)

        for file in files:
            path = os.path.join(self.rawdatadir, file)

            #print("Path", path, path.startswith("green"), "green" in path)
            if "yellow" in path: # or path.startswith("yellow"): #) and path.endswith('.parquet'):
                self.import_tripdata(path)

    
    def import_tripdata(self, file) -> None:
        processor = TripdataProcessor()
        
        print(file)

        df = pd.read_parquet(file)
        df = processor.process(df)
        
        print(file, len(df.index))
        
        df.to_sql(name='trips', con=self._con, if_exists='append', index=False)


        query = f"SELECT COUNT(*) FROM trips"
        cursor = self._con.execute(query)
        row_count = cursor.fetchone()[0]

        print("Rows in table", row_count)
        del df


    def _setup(self) -> None:
        self._create_tables()

    
    def _create_tables(self):
        for name, sql in TABLES.items():
            print("Create table: ", name)
            self._cursor.execute(sql)




    