from engine.loader import trip_data, location_data
from engine.cleaner import clean_negative_fees, clean_trip_duration, remove_duplicates
from engine.transformer import vendor_creation, ratecode_creation, payment_creation, datetime_creation, distance_creation, location_creation, trip_fact_creation
from engine.checker import key_validator
import engine.storer as storer
from engine.logger_config import setup_logger, log_execution_time
from datetime import datetime
import time
import gc

logger = setup_logger('main_pipeline')

@log_execution_time
def main():
    try:
        # Start timing and logging the pipeline execution
        pipeline_start_time = time.time()
        start_timestamp = datetime.now()
        logger.info("🚀 Initiating main pipeline...")
        logger.info("=" * 50)
        
        # PHASE 1: LOADING DATA
        logger.info("📥 PHASE 1: Loading from data sources...")
        df = trip_data()
        location_dim = location_data()
        logger.info("✅ All data loaded successfully")
        gc.collect()

        # PHASE 2: DATA CLEANING
        logger.info("🧹 PHASE 2: Perform validation and data cleaning...")
        df = clean_negative_fees(df) # Clean negative value in all fees columns
        df = clean_trip_duration(df) # Clean trip duration less than 2 minutes or more than 3 hours
        df = remove_duplicates(df) # Remove duplicate records and create trip_id
        logger.info("✅ Data cleaning completed")
        gc.collect()

        # PHASE 3: TRANSFORMING INTO STAR SCHEMA
        logger.info("⭐ PHASE 3: Transforming data into star schema...")
        vendor_dim = vendor_creation(df)
        ratecode_dim = ratecode_creation(df)
        payment_dim = payment_creation(df)
        datetime_dim = datetime_creation(df)
        distance_dim = distance_creation(df)
        location_dim = location_creation(location_dim)
        trip_fact = trip_fact_creation(df, datetime_dim, vendor_dim, ratecode_dim, payment_dim, distance_dim, location_dim)
        logger.info("✅ Star schema transformation completed")
        del df
        gc.collect()
        
        # PHASE 4: DATA QUALITY CHECKS
        logger.info("🔍 PHASE 4: Data Quality checks before storing...")
        key_validator(trip_fact)
        logger.info("✅ Data Quality checks passed")
        gc.collect()

        # PHASE 5: STORING DATA
        logger.info("💾 PHASE 5: Storing dataframes to Parquet and CSV formats...")
        storer.store_to_parquet(trip_fact, vendor_dim, ratecode_dim, payment_dim, distance_dim, datetime_dim, location_dim)
        logger.info("✅ Data stored successfully in Parquet format")
        gc.collect()
        storer.store_to_csv(vendor_dim, ratecode_dim, payment_dim, distance_dim, datetime_dim, trip_fact, location_dim)
        logger.info("✅ Data stored successfully in CSV format")
        logger.info("🎉 Pipeline completed successfully!")
        gc.collect()
        
        # End timing and logging calculation
        pipeline_end_time = time.time()
        end_timestamp = datetime.now()
        total_duration = pipeline_end_time - pipeline_start_time
        logger.info("=" * 50)
        logger.info("⏱️ PIPELINE TIMING SUMMARY:")
        logger.info(f"🚀 Started:  {start_timestamp.strftime('%H:%M:%S.%f')[:-3]}")
        logger.info(f"🎉 Finished: {end_timestamp.strftime('%H:%M:%S.%f')[:-3]}")
        logger.info(f"⏰ TOTAL DURATION: {total_duration:.2f} seconds")
        logger.info(f"⏰ TOTAL DURATION: {int(total_duration//60)}m {total_duration%60:.2f}s")
        logger.info("=" * 50)
        return "Pipeline completed successfully"
        
    except Exception as e:
        # Handle errors with timing
        if 'pipeline_start_time' in locals():
            error_duration = time.time() - pipeline_start_time
            logger.error(f"💥 PIPELINE FAILED AFTER: {error_duration:.2f} seconds: {str(e)}")
        else:
            logger.error(f"💥 PIPELINE FAILED: {str(e)}")
        logger.error("=" * 50)
        raise

if __name__ == "__main__":
    main()