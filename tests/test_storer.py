import engine.storer as storer
from pathlib import Path

class TestStoreToParquet:
    """Test cases for store_to_parquet function"""
    
    def test_store_to_parquet_creates_files(self, sample_trip_fact, temp_dir,
        sample_vendor_dim, sample_ratecode_dim, sample_payment_dim,
        sample_distance_dim, sample_datetime_dim, sample_location_dim):
        
        storer.store_to_parquet(
            sample_trip_fact,
            sample_vendor_dim,
            sample_ratecode_dim,
            sample_payment_dim,
            sample_distance_dim,
            sample_datetime_dim,
            sample_location_dim,
            base_dir=temp_dir
        )
        
        # Check if files were created
        parquet_dir = Path(temp_dir) / 'parquet'
        assert parquet_dir.exists()

class TestStoreToCsv:
    """Test cases for store_to_csv function"""
    
    def test_store_to_csv_creates_files(self, sample_trip_fact, temp_dir,
        sample_vendor_dim, sample_ratecode_dim, sample_payment_dim,
        sample_distance_dim, sample_datetime_dim, sample_location_dim):
        
        storer.store_to_csv(
           sample_trip_fact,
            sample_vendor_dim,
            sample_ratecode_dim,
            sample_payment_dim,
            sample_distance_dim,
            sample_datetime_dim,
            sample_location_dim,
            base_dir=temp_dir
        )

        # Check if CSV files were created
        csv_dir = Path(temp_dir) / 'csv'
        assert csv_dir.exists()