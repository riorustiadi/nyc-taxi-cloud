from engine.logger_config import setup_logger, log_execution_time

logger = setup_logger('cleaner')

@log_execution_time
def clean_negative_fees(df):
    df = df.copy()
    logger.info(f"Initial records: {len(df):,}")
    # Count negative values in fee columns
    fee_cols = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
    'improvement_surcharge', 'airport_fee', 'cbd_congestion_fee', 'congestion_surcharge']
    df.loc[:, 'fee_invalid'] = df[fee_cols].lt(0).any(axis=1)
    valid_count = (df['fee_invalid'] == False).sum()
    invalid_count = (df['fee_invalid'] == True).sum()

    # Print the counts
    logger.info(f"Valid records (no negative fees): {valid_count:,}")
    logger.info(f"Invalid records (has negative fees): {invalid_count:,}")

    # Clean negative fees
    df = df[df['fee_invalid'] == False].copy().reset_index(drop=True)
    logger.info(f"Valid records in df: {len(df):,}")
    return df

@log_execution_time
def clean_trip_duration(df):
    # Add column trip duration for Filter
    df.loc[:, 'duration'] = ((df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds() / 60).round().astype(int)

    # Filter out less than 2 minutes and more than 3 hours duration trips
    logger.info(f"Records before duration filter: {len(df):,}")
    valid_duration = df['duration'].between(2, 180, inclusive='neither')
    invalid_duration_count = (~valid_duration).sum()
    logger.info(f"Invalid records with duration less than 2 minutes and more than 3 hours: {invalid_duration_count:,}")
    df = df[valid_duration].copy().reset_index(drop=True)
    logger.info(f"Valid records after duration filter: {len(df):,}")
    return df

@log_execution_time
def remove_duplicates(df):
    df = df.drop_duplicates().reset_index(drop=True)
    logger.info(f"Records after removing duplicates: {len(df):,}")
    logger.info("Index reset after removing duplicates")
    return df