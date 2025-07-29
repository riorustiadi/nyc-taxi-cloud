import holidays
from engine.logger_config import setup_logger, log_execution_time

logger = setup_logger('transformer')

# Creating Vendor Dimension
@log_execution_time
def vendor_creation(df):
    logger.info("Creating Vendor Dimension...")
    vendor_mapping = {1: 'Creative Mobile Technologies LLC', 2: 'Curb Mobility LLC', 6: 'Myle Technologies Inc', 7: 'Helix'}
    vendor_dim = df[['vendor_id']].drop_duplicates().reset_index(drop=True)
    vendor_dim['vendor_key'] = range(1, len(vendor_dim) + 1)
    vendor_dim['vendor_name'] = vendor_dim['vendor_id'].map(vendor_mapping).fillna('Unknown')
    logger.info("Vendor Dimension created successfully ✅")
    logger.info(vendor_dim.info())
    return vendor_dim

# Creating Ratecode Dimension
@log_execution_time
def ratecode_creation(df):
    logger.info("Creating Ratecode Dimension...")
    ratecode_mapping = {1: 'Standard Rate', 2: 'JFK', 3: 'Newark', 4: 'Nassau or Westchester', 5: 'Negotiated Fare', 6: 'Group Ride', 99: 'Unknown'}
    ratecode_dim = df[['ratecode_id']].drop_duplicates().reset_index(drop=True)
    ratecode_dim['ratecode_key'] = range(1, len(ratecode_dim) + 1)
    ratecode_dim['ratecode_name'] = ratecode_dim['ratecode_id'].map(ratecode_mapping).fillna('Unknown')
    logger.info("Ratecode Dimension created successfully ✅")
    logger.info(ratecode_dim.info())
    return ratecode_dim

# Creating Payment Dimension
@log_execution_time
def payment_creation(df):
    logger.info("Creating Payment Dimension...")
    payment_mapping = {0: 'Flex Fare trip', 1: 'Credit card', 2: 'Cash', 3: 'No charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided trip'}
    payment_dim = df[['payment_id']].drop_duplicates().reset_index(drop=True)
    payment_dim['payment_key'] = range(1, len(payment_dim) + 1)
    payment_dim['payment_type'] = payment_dim['payment_id'].map(payment_mapping).fillna('Unknown')
    logger.info("Payment Dimension created successfully ✅")
    logger.info(payment_dim.info())
    return payment_dim

# Defining holidays for datetime dimension
us_ny_holidays = holidays.country_holidays('US', subdiv='NY')
def is_holiday(date):
    return date.date() in us_ny_holidays

# Creating Datetime Dimension
@log_execution_time
def datetime_creation(df):
    logger.info("Creating Datetime Dimension...")
    datetime_dim = df[['pickup_datetime', 'dropoff_datetime']].copy()
    datetime_dim['datetime_key'] = range(1, len(datetime_dim) + 1)
    datetime_dim['pickup_hour'] = datetime_dim['pickup_datetime'].dt.hour
    datetime_dim['pickup_day'] = datetime_dim['pickup_datetime'].dt.day
    datetime_dim['pickup_weekday'] = datetime_dim['pickup_datetime'].dt.weekday
    datetime_dim['pickup_month'] = datetime_dim['pickup_datetime'].dt.month
    datetime_dim['is_holiday'] = datetime_dim['pickup_datetime'].apply(is_holiday)
    logger.info("Datetime Dimension created successfully ✅")
    logger.info(datetime_dim.info())
    return datetime_dim

# Defining distance category
def distance_category(distance):
    if distance <= 2: return 'Short'
    elif 2 < distance <= 6: return 'Medium'
    else: return 'Long'

# Creating Distance Dimension
@log_execution_time
def distance_creation(df):
    logger.info("Creating Distance Dimension...")
    distance_dim = df[['trip_distance']].copy()
    distance_dim['distance_category'] = distance_dim['trip_distance'].apply(distance_category)
    distance_dim = distance_dim.drop_duplicates().reset_index(drop=True)
    distance_dim['distance_key'] = range(1, len(distance_dim) + 1)
    distance_dim = distance_dim[['distance_key', 'trip_distance', 'distance_category']]
    logger.info("Distance Dimension created successfully ✅")
    logger.info(distance_dim.info())
    return distance_dim

# Creating Location Dimension
@log_execution_time
def location_creation(location_dim):
    logger.info("Creating Location Dimension...")
    location_dim['location_key'] = range(1, len(location_dim) + 1)
    location_dim = location_dim[['location_key', 'location_id', 'zone', 'borough', 'service_zone']]
    logger.info("Location Dimension created successfully ✅")
    logger.info(location_dim.info())
    return location_dim

# Creating Fact Table
@log_execution_time
def trip_fact_creation(df, datetime_dim, vendor_dim, ratecode_dim, payment_dim, distance_dim, location_dim):
    logger.info("Creating Trip Fact Table...")
    trip_fact = df.copy()
    # Creating trip_id as primary key
    logger.info("Creating trip_id as primary key")
    trip_fact['trip_id'] = range(1, len(trip_fact) + 1)
    logger.info("Creating datetime_key for fact table")
    trip_fact['datetime_key'] = datetime_dim['datetime_key']

    # Merging dimension tables into fact table
    logger.info("Merging dimension tables into fact table")
    trip_fact = trip_fact.merge(vendor_dim[['vendor_id', 'vendor_key']], on='vendor_id', how='left')
    trip_fact = trip_fact.merge(ratecode_dim[['ratecode_id', 'ratecode_key']], on='ratecode_id', how='left')
    trip_fact = trip_fact.merge(payment_dim[['payment_id', 'payment_key']], on='payment_id', how='left')
    trip_fact = trip_fact.merge(distance_dim[['trip_distance', 'distance_key']], on='trip_distance', how='left')
    trip_fact = trip_fact.merge(
        location_dim[['location_id', 'location_key']], 
        left_on='pickup_location_id', 
        right_on='location_id', 
        how='left'
        ).rename(columns={'location_key': 'pickup_location_key'}).drop('location_id', axis=1)
    trip_fact = trip_fact.merge(
        location_dim[['location_id', 'location_key']], 
        left_on='dropoff_location_id', 
        right_on='location_id', 
        how='left'
        ).rename(columns={'location_key': 'dropoff_location_key'}).drop('location_id', axis=1)
    
    # Create a clean version of the fact table
    logger.info("Cleaning up fact table columns")
    fact_columns = [
    # Primary key
    'trip_id',
    # Foreign keys to dimensions
    'datetime_key',
    'vendor_key', 
    'ratecode_key',
    'payment_key',
    'distance_key',
    'pickup_location_key',
    'dropoff_location_key',
    # Fact Additional Attributes
    'passenger_count',
    'store_and_fwd',
    # Facts/Measures (numeric values)
    'fare_amount',
    'extra', 
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'congestion_surcharge',
    'airport_fee',
    'cbd_congestion_fee'
    ]
    trip_fact = trip_fact[fact_columns]
    logger.info("Trip Fact Table created successfully ✅")
    logger.info(trip_fact.info())
    return trip_fact