import os
import pandas as pd
from engine.logger_config import setup_logger, log_execution_time

logger = setup_logger('storer')

# Store to Parquet format with partitioning
@log_execution_time
def store_to_parquet(trip_fact, vendor_dim, ratecode_dim, payment_dim, distance_dim, datetime_dim, 
                     location_dim, base_dir='data'):
    try:
        logger.info("🚀 Start parquet storage process...")
        parquet_dir = os.path.join(base_dir, 'parquet', 'star_schema')
        os.makedirs(parquet_dir, exist_ok=True)
        logger.info("📁 Created target directory")

        # Define all tables
        star_schema_tables = {
            'trip_fact': trip_fact,
            'vendor_dim': vendor_dim,
            'ratecode_dim': ratecode_dim,
            'payment_dim': payment_dim,
            'distance_dim': distance_dim,
            'datetime_dim': datetime_dim,
            'location_dim': location_dim
        }

        # Save tables
        for table_name, table_df in star_schema_tables.items():
            logger.info(f"📥 Storing table {table_name} ({len(table_df):,} records)")
            
            if table_name == 'trip_fact':                
                # Get datetime info for partitioning
                logger.info("🔄 Adding partitioning to trip_fact...")
                datetime_info = datetime_dim.set_index('datetime_key')[['pickup_datetime']]
                fact_with_partition = table_df.merge(
                    datetime_info, 
                    left_on='datetime_key', 
                    right_index=True, 
                    how='left'
                )
                # Ensure pickup_datetime is datetime type before .dt operations
                if not pd.api.types.is_datetime64_any_dtype(fact_with_partition['pickup_datetime']):
                    logger.warning("⚠️ Converting pickup_datetime to datetime type...")
                    fact_with_partition['pickup_datetime'] = pd.to_datetime(fact_with_partition['pickup_datetime'])
                
                # Add partition columns
                fact_with_partition['year'] = fact_with_partition['pickup_datetime'].dt.year
                fact_with_partition['month'] = fact_with_partition['pickup_datetime'].dt.month
                
                # Drop datetime before saving (it's in datetime_dim)
                fact_with_partition = fact_with_partition.drop('pickup_datetime', axis=1)
                
                # Save with partitioning
                fact_with_partition.to_parquet(
                    os.path.join(parquet_dir, f'{table_name}'),
                    partition_cols=['year', 'month'],
                    engine='pyarrow',
                    compression='snappy',
                    index=False
                )
                logger.info(f"✅ {table_name} saved with partitioning: {table_df.shape}")
                
            else:
                # Save dimension tables
                table_df.to_parquet(
                    os.path.join(parquet_dir, f'{table_name}.parquet'),
                    engine='pyarrow',
                    compression='snappy', 
                    index=False
                )
                logger.info(f"✅ {table_name} saved: {table_df.shape}")

        logger.info("\n🎉 Complete Star Schema with Location Information Saved!")
        logger.info("\nStar Schema Summary:")
        logger.info("📊 Fact Table: trip_fact (partitioned by year/month)")
        logger.info("📋 Dimension Tables: vendor_dim, ratecode_dim, payment_dim, distance_dim, datetime_dim, location_dim")
    
    except Exception as e:
        logger.error(f"❌ Error during Parquet storage: {str(e)}")
        raise

# Store to CSV format
@log_execution_time
def store_to_csv(vendor_dim, ratecode_dim, payment_dim, distance_dim, datetime_dim, 
                 trip_fact, location_dim, base_dir='data'):
    try:
        # Create directory structure
        logger.info("🚀 Starting CSV storage process...")
        csv_dir = os.path.join(base_dir, 'csv', 'star_schema')
        os.makedirs(csv_dir, exist_ok=True)
        logger.info("📁 Target directory created")

        # Define all tables to save
        star_schema_tables = {
            'vendor_dim': vendor_dim,
            'ratecode_dim': ratecode_dim,
            'payment_dim': payment_dim,
            'distance_dim': distance_dim,
            'location_dim': location_dim,
            'datetime_dim': datetime_dim,
            'trip_fact': trip_fact
        }

        logger.info(f"📊 Total tables to save: {len(star_schema_tables)}")

        # Save each table to CSV
        logger.info("🚀 Storing data to CSV...")

        for table_name, table_df in star_schema_tables.items():
            logger.info(f"📥 Saving {table_name} ({len(table_df):,} records)")
            try:
                # Save to CSV
                csv_path = os.path.join(csv_dir, f'{table_name}.csv')
                table_df.to_csv(csv_path, index=False, encoding='utf-8')
                # table_df.to_csv(csv_path, index=False, encoding='utf-8-sig')  # For Excel compatibility
                # table_df.to_csv(csv_path, index=False, compression='gzip')  # Saves as .csv.gz
                logger.info(f"✅ {table_name}: {len(table_df):,} rows saved to {csv_path}")
            except Exception as e:
                logger.error(f"❌ Error saving {table_name}: {str(e)}")

        logger.info("\n🎉 CSV export completed!")

        # Verify saved files
        logger.info("\n🔍 Verifying saved CSV files...")

        for table_name in star_schema_tables.keys():
            csv_path = os.path.join(csv_dir, f'{table_name}.csv')
            
            if os.path.exists(csv_path):
                file_size = os.path.getsize(csv_path) / (1024*1024)  # Size in MB
                logger.info(f"✅ {table_name}.csv: {file_size:.2f} MB")
            else:
                logger.error(f"❌ {table_name}.csv: File not found")

    except Exception as e:
        logger.error(f"❌ Error during CSV export: {str(e)}")
        raise