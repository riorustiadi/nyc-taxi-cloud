import pandas as pd
import numpy as np
import engine.cleaner as cleaner

class TestCleanNegativeFees:
    """Test cases for clean_negative_fees function"""
    
    def test_clean_negative_fees_removes_negative(self, corrupted_trip_data):
        """Test that negative fees are properly removed"""
        initial_count = len(corrupted_trip_data)
        
        result = cleaner.clean_negative_fees(corrupted_trip_data)
        
        # Should have fewer records after cleaning
        assert len(result) < initial_count
        
        # No negative values should remain
        fee_columns = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'airport_fee', 'cbd_congestion_fee', 'congestion_surcharge']
        for col in fee_columns:
            if col in result.columns:
                assert (result[col] >= 0).all()

    def test_clean_negative_fees_preserves_positive(self, sample_trip_data):
        """Test that valid data is preserved"""
        # Manually ensure all values are positive
        fee_columns = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'airport_fee', 'cbd_congestion_fee', 'congestion_surcharge']
        for col in fee_columns:
            sample_trip_data[col] = np.abs(sample_trip_data[col])
        
        initial_count = len(sample_trip_data)
        result = cleaner.clean_negative_fees(sample_trip_data)
        
        # All positive stay positive
        assert len(result) == initial_count

class TestCleanTripDuration:
    """Test cases for clean_trip_duration function"""
    
    def test_clean_trip_duration_removes_invalid(self):
        """Test removal of trips with invalid duration"""
        data = {
            'pickup_datetime': pd.to_datetime([
                '2024-01-01 10:00:00',  # Valid trip (30 min)
                '2024-01-01 11:00:00',  # Too short (1 min)
                '2024-01-01 12:00:00',  # Too long (5 hours)
                '2024-01-01 13:00:00'   # Valid trip (45 min)
            ]),
            'dropoff_datetime': pd.to_datetime([
                '2024-01-01 10:30:00',
                '2024-01-01 11:01:00',
                '2024-01-01 17:00:00',
                '2024-01-01 13:45:00'
            ]),
            'VendorID': [1, 2, 1, 2]
        }
        df = pd.DataFrame(data)
        
        result = cleaner.clean_trip_duration(df)
        
        # Should only keep valid duration trips (2 out of 4)
        assert len(result) == 2

class TestRemoveDuplicates:
    """Test cases for remove_duplicates function"""
            
    def test_remove_duplicates(self):
        """Test actual duplicate removal"""
        # Create data with duplicates
        data = {
            'VendorID': [1, 1, 2],  # First two are duplicates
            'tpep_pickup_datetime': pd.to_datetime([
                '2024-01-01 10:00:00',
                '2024-01-01 10:00:00',  # Duplicate
                '2024-01-01 11:00:00'
            ]),
            'tpep_dropoff_datetime': pd.to_datetime([
                '2024-01-01 10:30:00',
                '2024-01-01 10:30:00',  # Duplicate
                '2024-01-01 11:30:00'
            ]),
            'fare_amount': [10.0, 10.0, 15.0]  # Duplicate
        }
        df = pd.DataFrame(data)
        
        result = cleaner.remove_duplicates(df)
        
        # Should remove duplicates
        assert len(result) == 2  # Only 2 unique records