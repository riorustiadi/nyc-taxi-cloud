import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tempfile
import os
import sys

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

@pytest.fixture
def sample_trip_data():
    """Create sample trip data AFTER loader.py processing (with renamed columns)"""
    np.random.seed(42)
    n_records = 1000
    
    # EXACT column names AFTER loader.py renaming
    data = {
        # RENAMED columns (match loader.py output)
        'vendor_id': np.random.choice([1, 2, 3], n_records),                    
        'pickup_datetime': pd.date_range('2024-01-01', periods=n_records, freq='h'),     
        'dropoff_datetime': pd.date_range('2024-01-01 00:30:00', periods=n_records, freq='h'), 
        'passenger_count': np.random.choice([1, 2, 3, 4, 5], n_records),       
        'trip_distance': np.random.uniform(0.1, 20.0, n_records),              
        'ratecode_id': np.random.choice([1, 2, 3, 4, 5, 6], n_records),        
        'store_and_fwd': np.random.choice(['Y', 'N'], n_records),               
        'pickup_location_id': np.random.randint(1, 266, n_records),            
        'dropoff_location_id': np.random.randint(1, 266, n_records),           
        'payment_id': np.random.choice([1, 2, 3, 4], n_records),               
        
        # Fee columns
        'fare_amount': np.random.uniform(2.5, 50.0, n_records),
        'extra': np.random.uniform(0, 2.0, n_records),
        'mta_tax': np.random.uniform(0, 0.5, n_records),
        'tip_amount': np.random.uniform(0, 10.0, n_records),
        'tolls_amount': np.random.uniform(0, 5.0, n_records),
        'improvement_surcharge': np.random.uniform(0, 0.3, n_records),
        'total_amount': np.random.uniform(5.0, 60.0, n_records),
        'congestion_surcharge': np.random.uniform(0, 2.5, n_records),
        'airport_fee': np.random.uniform(0, 1.25, n_records),                  
        'cbd_congestion_fee': np.random.uniform(0, 2.5, n_records)             
    }
    
    return pd.DataFrame(data)

@pytest.fixture
def sample_location_data():
    """Create sample location data AFTER loader.py processing (with renamed columns)"""
    data = {
        # RENAMED columns (match loader.py output)
        'location_id': [1, 2, 3, 4, 5],                                        
        'borough': ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'], 
        'zone': ['Central Park', 'Downtown Brooklyn', 'JFK Airport', 'Yankee Stadium', 'St. George'], 
        'service_zone': ['Yellow Zone', 'Boro Zone', 'Airports', 'Boro Zone', 'Boro Zone']  
    }
    return pd.DataFrame(data)

@pytest.fixture
def corrupted_trip_data():
    """Create corrupted data with CORRECT column names after loader processing"""
    data = {
        # Use RENAMED column names (after loader.py processing)
        'vendor_id': [1, 2, 1, 2],                                             
        'pickup_datetime': ['2024-01-01 10:00:00', '2024-01-01 11:00:00', 
                           '2024-01-01 12:00:00', '2024-01-01 13:00:00'],       
        'dropoff_datetime': ['2024-01-01 10:30:00', '2024-01-01 11:01:00', 
                            '2024-01-01 15:30:00', '2024-01-01 13:30:00'],      
        
        # Fee columns with negative values for testing cleaner
        'fare_amount': [10.0, -5.0, 15.0, 20.0],              # Negative value
        'extra': [0.5, 1.0, -0.5, 1.5],                       # Negative value
        'mta_tax': [0.5, 0.5, 0.5, -0.5],                     # Negative value
        'tip_amount': [2.0, 1.0, -1.0, 3.0],                  # Negative value
        'tolls_amount': [0.0, 2.0, 1.0, -1.0],                # Negative value
        'improvement_surcharge': [0.3, 0.3, 0.3, 0.3],        
        'total_amount': [12.0, 6.0, 14.0, 23.0],
        'congestion_surcharge': [2.5, 2.5, -1.0, 2.5],        # Negative value
        'airport_fee': [0.0, 1.25, 0.0, -0.5],                # Negative value
        'cbd_congestion_fee': [0.0, 2.5, -1.0, 2.5],          # Negative value
        
        # Additional required columns
        'trip_distance': [2.0, 1.5, 10.0, 3.0],
        'passenger_count': [1, 2, 1, 1],
        'ratecode_id': [1, 1, 1, 1],                          
        'store_and_fwd': ['N', 'N', 'N', 'Y'],                  
        'pickup_location_id': [161, 239, 132, 236],           
        'dropoff_location_id': [239, 161, 165, 161],          
        'payment_id': [1, 2, 1, 2]                            
    }
    df = pd.DataFrame(data)
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])              
    df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])           
    return df

@pytest.fixture
def temp_dir():
    """Create temporary directory for testing file operations"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

# Raw data fixtures (before loader processing)
@pytest.fixture
def sample_raw_trip_data():
    """Create sample trip data BEFORE loader.py processing (original column names)"""
    np.random.seed(42)
    n_records = 100
    
    # All original column names (before loader renaming)
    data = {
        # Core columns (original names)
        'VendorID': np.random.choice([1, 2, 3], n_records),                    
        'tpep_pickup_datetime': pd.date_range('2024-01-01', periods=n_records, freq='h'),     
        'tpep_dropoff_datetime': pd.date_range('2024-01-01 00:30:00', periods=n_records, freq='h'), 
        'passenger_count': np.random.choice([1, 2, 3, 4, 5], n_records),
        'trip_distance': np.random.uniform(0.1, 20.0, n_records),
        'RatecodeID': np.random.choice([1, 2, 3, 4, 5, 6], n_records),        
        'store_and_fwd_flag': np.random.choice(['Y', 'N'], n_records),         
        'PULocationID': np.random.randint(1, 266, n_records),                 
        'DOLocationID': np.random.randint(1, 266, n_records),                
        'payment_type': np.random.choice([1, 2, 3, 4], n_records),            
        
        # All fee columns (original names)
        'fare_amount': np.random.uniform(2.5, 50.0, n_records),
        'extra': np.random.uniform(0, 2.0, n_records),                        
        'mta_tax': np.random.uniform(0, 0.5, n_records),                      
        'tip_amount': np.random.uniform(0, 10.0, n_records),
        'tolls_amount': np.random.uniform(0, 5.0, n_records),                 
        'improvement_surcharge': np.random.uniform(0, 0.3, n_records),        
        'total_amount': np.random.uniform(5.0, 60.0, n_records),              
        'congestion_surcharge': np.random.uniform(0, 2.5, n_records),        
        'Airport_fee': np.random.uniform(0, 1.25, n_records),                 
        'cbd_congestion_fee': np.random.uniform(0, 2.5, n_records)            
    }
    
    return pd.DataFrame(data)

@pytest.fixture
def sample_raw_location_data():
    """Create sample location data BEFORE loader.py processing (original column names)"""
    data = {
        # Original column names (before loader renaming)
        'LocationID': [1, 2, 3, 4, 5],                                        
        'Borough': ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'], 
        'Zone': ['Central Park', 'Downtown Brooklyn', 'JFK Airport', 'Yankee Stadium', 'St. George'], 
        'service_zone': ['Yellow Zone', 'Boro Zone', 'Airports', 'Boro Zone', 'Boro Zone']  
    }
    return pd.DataFrame(data)

@pytest.fixture
def sample_vendor_dim():
    return pd.DataFrame({'vendor_id': [1, 2], 'vendor_name': ['A', 'B']})

@pytest.fixture
def sample_ratecode_dim():
    return pd.DataFrame({'ratecode_id': [1, 2], 'ratecode_name': ['Standard', 'JFK']})

@pytest.fixture
def sample_payment_dim():
    return pd.DataFrame({'payment_id': [1, 2], 'payment_type': ['Credit', 'Cash']})

@pytest.fixture
def sample_distance_dim():
    return pd.DataFrame({'distance_key': [1, 2], 'distance_group': ['short', 'long']})

@pytest.fixture
def sample_datetime_dim():
    return pd.DataFrame({
        'datetime_key': [1, 2],
        'pickup_datetime': [pd.Timestamp('2024-01-01 10:00:00'), 
                            pd.Timestamp('2024-01-01 11:00:00')]
    })

@pytest.fixture
def sample_location_dim():
    return pd.DataFrame({
        'location_key': [1, 2],
        'location_id': [1, 2],
        'borough': ['Manhattan', 'Brooklyn'],
        'zone': ['Central Park', 'Downtown Brooklyn'],
        'service_zone': ['Yellow Zone', 'Boro Zone']
    })

@pytest.fixture
def sample_trip_fact():
    """Minimal trip_fact fixture for storer tests (must have datetime_key)"""
    return pd.DataFrame({
        'trip_id': [1, 2, 3],
        'datetime_key': [1, 2, 1],
        'vendor_id': [1, 2, 1],
        'fare_amount': [10.5, 20.0, 15.0]
    })