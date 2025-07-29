import pandas as pd
import engine.transformer as transformer

class TestVendorCreation:
    """Test cases for vendor_creation function"""
    
    def test_vendor_creation_structure(self, sample_trip_data):
        """Test vendor dimension structure"""
        result = transformer.vendor_creation(sample_trip_data)
        
        assert isinstance(result, pd.DataFrame)
        assert 'vendor_key' in result.columns
        assert 'vendor_id' in result.columns
        assert 'vendor_name' in result.columns
        assert len(result) > 0
        # Check for unique vendor keys 
        assert result['vendor_key'].nunique() == len(result)

class TestDatetimeCreation:
    """Test cases for datetime_creation function"""
    
    def test_datetime_creation_structure(self, sample_trip_data):
        """Test datetime dimension structure"""
        result = transformer.datetime_creation(sample_trip_data)
        
        expected_columns = [
            'datetime_key', 'pickup_datetime', 'dropoff_datetime',
            'pickup_hour', 'pickup_day', 'pickup_weekday', 'pickup_month', 'is_holiday'
        ]
        for col in expected_columns:
            assert col in result.columns
        assert isinstance(result, pd.DataFrame)
        
        # Check for datetime data types
        assert pd.api.types.is_datetime64_any_dtype(result['pickup_datetime'])
        assert pd.api.types.is_datetime64_any_dtype(result['dropoff_datetime'])
        assert pd.api.types.is_integer_dtype(result['pickup_hour'])
        assert pd.api.types.is_integer_dtype(result['pickup_day'])
        # Check for unique datetime keys
        assert result['datetime_key'].nunique() == len(result)

class TestRatecodeCreation:
    """Test cases for ratecode_creation function"""
    
    def test_ratecode_creation_structure(self, sample_trip_data):
        """Test ratecode dimension structure"""
        result = transformer.ratecode_creation(sample_trip_data)
        
        assert isinstance(result, pd.DataFrame)
        assert 'ratecode_key' in result.columns
        assert 'ratecode_id' in result.columns
        assert 'ratecode_name' in result.columns
        assert len(result) > 0
        # Check for unique ratecode keys 
        assert result['ratecode_key'].nunique() == len(result)
        
class TestPaymentCreation:
    """Test cases for payment_creation function"""
    
    def test_payment_creation_structure(self, sample_trip_data):
        """Test payment dimension structure"""
        result = transformer.payment_creation(sample_trip_data)
        
        assert isinstance(result, pd.DataFrame)
        assert 'payment_key' in result.columns
        assert 'payment_id' in result.columns
        assert 'payment_type' in result.columns
        assert len(result) > 0
        # Check for unique payment keys 
        assert result['payment_key'].nunique() == len(result)
        
class TestDistanceCreation:
    """Test cases for distance_creation function"""
    
    def test_distance_creation_structure(self, sample_trip_data):
        """Test distance dimension structure"""
        result = transformer.distance_creation(sample_trip_data)
        
        assert isinstance(result, pd.DataFrame)
        assert 'distance_key' in result.columns
        assert 'trip_distance' in result.columns
        assert 'distance_category' in result.columns
        assert len(result) > 0
        # Check for unique distance keys 
        assert result['distance_key'].nunique() == len(result)
        
class TestLocationCreation:
    """Test cases for location_creation function"""
    
    def test_location_creation_structure(self, sample_location_data):
        """Test location dimension structure"""
        result = transformer.location_creation(sample_location_data)

        assert isinstance(result, pd.DataFrame)
        assert 'location_key' in result.columns
        assert 'location_id' in result.columns
        assert 'borough' in result.columns
        assert 'zone' in result.columns
        assert 'service_zone' in result.columns
        assert len(result) > 0
        # Check for unique location keys 
        assert result['location_key'].nunique() == len(result)

class TestTripFactCreation:
    """Test cases for trip_fact_creation function"""
    
    def test_trip_fact_creation_with_all_dimensions(self, sample_trip_data, sample_location_data):
        """Test trip fact creation with all required dimensions"""
        # Create sample dimensions
        datetime_dim = transformer.datetime_creation(sample_trip_data)
        vendor_dim = transformer.vendor_creation(sample_trip_data)
        ratecode_dim = transformer.ratecode_creation(sample_trip_data)
        payment_dim = transformer.payment_creation(sample_trip_data)
        distance_dim = transformer.distance_creation(sample_trip_data)
        location_dim = transformer.location_creation(sample_location_data)
        
        result = transformer.trip_fact_creation(
            sample_trip_data, datetime_dim, vendor_dim, ratecode_dim,
            payment_dim, distance_dim, location_dim
        )
                
        # Check for key columns
        fact_columns = [
            # Primary keys
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
        
        for col in fact_columns:
            assert col in result.columns
        assert isinstance(result, pd.DataFrame)
        # Check for unique primary keys
        assert result['trip_id'].nunique() == len(result)
        assert len(result) > 0