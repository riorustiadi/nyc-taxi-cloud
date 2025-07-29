import pandas as pd
from engine.logger_config import setup_logger, log_execution_time

logger = setup_logger('loader')

@log_execution_time
def trip_data():
    try:
        logger.info("🚀 Loading trip data...")
        df = pd.read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet")
        logger.info(f"✅ Trip data loaded successfully: {len(df):,} records")

        # Rename columns for consistency in fact table
        logger.info("🔄 Renaming columns for consistency...")
        df.rename(columns={'VendorID': 'vendor_id',
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'RatecodeID': 'ratecode_id',
                    'store_and_fwd_flag': 'store_and_fwd',
                    'PULocationID': 'pickup_location_id',
                    'DOLocationID': 'dropoff_location_id',
                    'payment_type': 'payment_id',
                    'Airport_fee': 'airport_fee'}, inplace=True)
        logger.info(f"✅ Column renaming completed. Shape: {df.shape}")
        logger.debug(f"Columns: {list(df.columns)}")

        return df
    
    except Exception as e:
        logger.error(f"❌ Error loading trip data: {str(e)}")
        raise

@log_execution_time
def location_data():
    try:
        logger.info("🚀 Loading location data...")
        location_dim = pd.read_csv("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")
        logger.info(f"✅ Location data loaded successfully: {len(location_dim):,} records")

        # Rename columns for consistency in location dimension
        logger.info("🔄 Renaming columns for consistency...")
        location_dim.rename(columns={
            'LocationID': 'location_id',
            'Borough': 'borough',
            'Zone': 'zone'}, inplace=True)
        logger.info(f"✅ Column renaming completed. Shape: {location_dim.shape}")
        
        return location_dim
    
    except Exception as e:
        logger.error(f"❌ Error loading location data: {str(e)}")
        raise