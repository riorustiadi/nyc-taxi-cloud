import pytest
import requests
from unittest.mock import patch
import engine.loader as loader

class TestTripData:
    """Test cases for trip_data function"""
    
    def test_trip_data_network_error(self):
        """Test handling of network errors"""
        with patch('pandas.read_parquet') as mock_read:
            mock_read.side_effect = requests.exceptions.ConnectionError("Network error")
            
            with pytest.raises(requests.exceptions.ConnectionError):
                loader.trip_data()
    
    def test_trip_data_column_renaming(self, sample_raw_trip_data):
        """Test column renaming functionality"""
        with patch('pandas.read_parquet') as mock_read:
            # Create data with old column names using raw trip data
            mock_read.return_value = sample_raw_trip_data
            
            result = loader.trip_data()
            
            assert 'pickup_datetime' in result.columns
            assert 'dropoff_datetime' in result.columns
            assert 'ratecode_id' in result.columns
            assert 'store_and_fwd' in result.columns
            assert 'pickup_location_id' in result.columns
            assert 'dropoff_location_id' in result.columns
            assert 'payment_id' in result.columns
            assert 'airport_fee' in result.columns

class TestLocationData:
    """Test cases for location_data function"""
    
    def test_location_data_network_error(self):
        """Test handling of network errors for location data"""
        with patch('pandas.read_csv') as mock_read:
            mock_read.side_effect = requests.exceptions.ConnectionError("Network error")
            
            with pytest.raises(requests.exceptions.ConnectionError):
                loader.location_data()
    
    def test_location_data_column_renaming(self, sample_raw_location_data):
        """Test location data column renaming"""
        with patch('pandas.read_csv') as mock_read:
            mock_read.return_value = sample_raw_location_data
            
            result = loader.location_data()
            
            # Check if columns are properly renamed
            expected_columns = ['location_id', 'borough', 'zone', 'service_zone']
            for col in expected_columns:
                assert col in result.columns