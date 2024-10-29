import numpy as np

class Processor:
    ...

class TripdataProcessor(Processor):
    
    def process(self, df):
        df = df.pipe(self.rename_columns) \
            .pipe(self.norm_columns) \
            .pipe(self.add_columns) \
            .pipe(self.order_columns)
        return df

    
    def rename_columns(self, df):
        df = df.rename(columns={
            'VendorID': 'vendor_id',
            "lpep_pickup_datetime": "pickup_at",
            "tpep_pickup_datetime": "pickup_at",
            "lpep_dropoff_datetime": "dropoff_at",
            "tpep_dropoff_datetime": "dropoff_at",
            "RatecodeID": "ratecode_id",
            "PULocationID": "pickup_location_id",
            "DOLocationID": "dropoff_location_id",
            'trip_distance': 'trip_distance_mi'
        })
        return df

    def norm_columns(self, df):
        df.columns = [c.replace(" ", "_").lower() for c in df.columns]
        return df

    def add_columns(self, df, trip_category=np.nan):
        if "airport_fee" not in df.columns:
            df["airport_fee"] = np.nan
    
        if "trip_type" not in df.columns:
            df["trip_type"] = np.nan
    
        if "ehail_fee" not in df.columns:
            df["ehail_fee"] = np.nan
    
        if "trip_category" not in df.columns:
            df["trip_category"] = trip_category
            
        return df

    def order_columns(self, df):
        df = df[[
            'vendor_id',
            'pickup_at', 'dropoff_at',
            'pickup_location_id', 'dropoff_location_id',
            'store_and_fwd_flag',
            'ratecode_id', 
            'passenger_count', 
            'trip_distance_mi',
            'extra',
            'mta_tax',
            'fare_amount', 
            'tolls_amount', 
            'tip_amount', 
            'total_amount', 
            'payment_type', 
            'trip_type',
            'trip_category',
            'improvement_surcharge',
            'congestion_surcharge',
            'ehail_fee', 
            'airport_fee']]
        return df
