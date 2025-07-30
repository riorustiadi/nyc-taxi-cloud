import os
import pandas as pd
from engine.logger_config import setup_logger, log_execution_time

logger = setup_logger('storer')

# To append dataframe to existing file
def append_to_existing_file(new_df, file_path, file_format='parquet'):
    try:
        if os.path.exists(file_path):
            if file_format == 'parquet':
                existing_df = pd.read_parquet(file_path)
            else:  # CSV
                existing_df = pd.read_csv(file_path, low_memory=False, 
                                          dtype={'store_and_fwd': 'str'})
            
            # Combine old and new data
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            return combined_df
        else:
            return new_df
    except Exception as e:
        logger.warning(f"Could not read existing file {file_path}: {e}")
        return new_df

# Store to Parquet format
@log_execution_time
def store_to_parquet(trip_fact, vendor_dim, ratecode_dim, payment_dim, distance_dim, datetime_dim, 
                     location_dim, base_dir='data', append_mode=True):
    try:
        logger.info("🚀 Start parquet storage process...")
        parquet_dir = os.path.join(base_dir, 'parquet', 'star_schema')
        os.makedirs(parquet_dir, exist_ok=True)
        logger.info("📁 Opening target directory")

        # Tables that need append
        append_tables = {
            'trip_fact': trip_fact,
            'datetime_dim': datetime_dim,
            'distance_dim': distance_dim
        }
        
        # Tables that can be overwritten (dimensions are usually static)
        static_tables = {
            'vendor_dim': vendor_dim,
            'ratecode_dim': ratecode_dim,
            'payment_dim': payment_dim,
            'location_dim': location_dim
        }

        # Handle append tables
        if append_mode:
            for table_name, table_df in append_tables.items():
                file_path = os.path.join(parquet_dir, f'{table_name}.parquet')
                combined_df = append_to_existing_file(table_df, file_path, 'parquet')
                
                combined_df.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
                logger.info(f"✅ {table_name} appended: {len(combined_df):,} total records")
        
        # Handle static tables (overwrite)
        for table_name, table_df in static_tables.items():
            file_path = os.path.join(parquet_dir, f'{table_name}.parquet')
            table_df.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
            logger.info(f"✅ {table_name} saved: {table_df.shape}")

    except Exception as e:
        logger.error(f"❌ Error during Parquet storage: {str(e)}")
        raise

# Store to CSV format
@log_execution_time
def store_to_csv(vendor_dim, ratecode_dim, payment_dim, distance_dim, datetime_dim, 
                 trip_fact, location_dim, base_dir='data', append_mode=True):
    try:
        # Create directory structure
        logger.info("🚀 Starting CSV storage process...")
        csv_dir = os.path.join(base_dir, 'csv', 'star_schema')
        os.makedirs(csv_dir, exist_ok=True)
        logger.info("📁 Checking target directory")

        # Tables that need append
        append_tables = {
            'trip_fact': trip_fact,
            'datetime_dim': datetime_dim,
            'distance_dim': distance_dim
        }
        
        # Tables that can be overwritten (static dimensions)
        static_tables = {
            'vendor_dim': vendor_dim,
            'ratecode_dim': ratecode_dim,
            'payment_dim': payment_dim,
            'location_dim': location_dim
        }

        # Handle append tables
        if append_mode:
            for table_name, table_df in append_tables.items():
                csv_path = os.path.join(csv_dir, f'{table_name}.csv')
                combined_df = append_to_existing_file(table_df, csv_path, 'csv')
                combined_df.to_csv(csv_path, index=False, encoding='utf-8')
                logger.info(f"✅ {table_name} appended: {len(combined_df):,} total records")
        
        # Handle static tables (overwrite)
        for table_name, table_df in static_tables.items():
            csv_path = os.path.join(csv_dir, f'{table_name}.csv')
            table_df.to_csv(csv_path, index=False, encoding='utf-8')
            logger.info(f"✅ {table_name} saved: {table_df.shape}")

    except Exception as e:
        logger.error(f"❌ Error during CSV export: {str(e)}")
        raise