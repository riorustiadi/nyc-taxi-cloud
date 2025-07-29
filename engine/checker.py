from engine.logger_config import setup_logger, log_execution_time

logger = setup_logger('checker')

# Check for missing foreign keys
@log_execution_time
def key_validator(trip_fact):
    logger.info("🔍 Data Quality Check - Missing Foreign Keys:")
    
    quality_checks = {
        'vendor_key': trip_fact['vendor_key'].isnull().sum(),
        'ratecode_key': trip_fact['ratecode_key'].isnull().sum(),
        'payment_key': trip_fact['payment_key'].isnull().sum(),
        'distance_key': trip_fact['distance_key'].isnull().sum(),
        'pickup_location_key': trip_fact['pickup_location_key'].isnull().sum(),
        'dropoff_location_key': trip_fact['dropoff_location_key'].isnull().sum(),
        'datetime_key': trip_fact['datetime_key'].isnull().sum()
    }

    for key_name, missing_count in quality_checks.items():
        status = "✅" if missing_count == 0 else "⚠️"
        logger.info(f"{status} {key_name}: {missing_count:,} missing values")

    # Show sample of fact table
    logger.info("\n📋 Sample Fact Table:")
    logger.info(trip_fact.head())
    return quality_checks