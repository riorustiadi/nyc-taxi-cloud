# NYC Taxi Cloud Data Pipeline

🚖 A professional-grade ETL pipeline for processing NYC Taxi data using Python, designed with modular architecture, comprehensive testing, and star schema data warehousing.

## ✨ Technical Highlights

- **Modular ETL Architecture**: Clean separation of concerns with dedicated loader, cleaner, transformer, and storer components
- **Star Schema Implementation**: Professional data warehouse design with surrogate keys and dimensional modeling
- **Comprehensive Testing**: Fixture-based testing infrastructure with pytest covering all pipeline components
- **Production-Ready Logging**: Execution timing, detailed logging, and error handling throughout the pipeline
- **Flexible Storage**: Configurable output supporting both local and cloud deployment (Parquet + CSV formats)
- **Data Quality Assurance**: Built-in validation and cleaning operations for enterprise-grade data integrity

## 📁 Project Structure

```
nyc-taxi-cloud/
├── 📋 main.py                     # Pipeline orchestration and entry point
├── 📦 engine/                     # Core ETL components package
│   ├── 🔧 __init__.py            # Package initialization
│   ├── 📥 loader.py              # Data loading from NYC Taxi API
│   ├── 🧹 cleaner.py             # Data quality and cleaning operations
│   ├── ⭐ transformer.py         # Star schema transformation logic
│   ├── 💾 storer.py              # Flexible storage (Parquet/CSV)
│   ├── 🔍 checker.py             # Data validation and quality checks
│   └── 📊 logger_config.py       # Centralized logging configuration
├── 🧪 tests/                     # Comprehensive test suite
│   ├── 🔧 __init__.py            # Test package initialization
│   ├── 🎯 conftest.py            # Test fixtures and configuration
│   ├── 🚀 test_main_pipeline.py  # End-to-end pipeline testing
│   ├── 🧹 test_cleaner.py        # Data cleaning function tests
│   ├── 📥 test_loader.py         # Data loading function tests
│   ├── ⭐ test_transformer.py    # Transformation logic tests
│   └── 💾 test_storer.py         # Storage functionality tests
├── 📋 requirements.txt            # Python dependencies
├── 🚫 .gitignore                 # Git ignore configuration
└── 📚 README.md                  # Project documentation
```

## 🚀 Quick Start

### Prerequisites

- Python 3.13.5+
- Windows PowerShell / Command Prompt

### Setup Instructions

1. **Clone and Navigate to Project**
```powershell
cd c:\Users\USER\PORTO\nyc-taxi-cloud
```

2. **Create and Activate Virtual Environment**
```powershell
python -m venv envcloud
.\envcloud\Scripts\Activate.ps1
```

3. **Install Dependencies**
```powershell
pip install -r requirements.txt
```

4. **Run the Pipeline**
```powershell
python main.py
```

5. **Run Tests**
```powershell
pytest tests/ -v
```

## 🗄️ Data Sources

- **Trip Data**: NYC TLC Yellow Taxi Trip Records (January 2025)
  - Source: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet`
  - Format: Parquet file with ~3M+ records
  
- **Location Data**: NYC Taxi Zone Lookup Table
  - Source: `https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv`
  - Format: CSV with borough, zone, and service zone information

## ⭐ Star Schema Design

The pipeline transforms raw taxi data into a dimensional data warehouse with the following structure:

### Fact Table
- **`trip_fact`**: Core business events with surrogate keys

### Dimension Tables
- **`vendor_dim`**: Taxi vendor information with vendor_key
- **`ratecode_dim`**: Rate code types with ratecode_key  
- **`payment_dim`**: Payment methods with payment_key
- **`datetime_dim`**: Date/time dimensions with datetime_key
- **`distance_dim`**: Trip distance categories with distance_key
- **`location_dim`**: NYC taxi zones with location_key

## 🔄 Pipeline Phases

1. **🚀 PHASE 1: Data Loading**
   - Fetch trip data from NYC TLC API
   - Load location dimension data
   - Column standardization and renaming

2. **🧹 PHASE 2: Data Cleaning**
   - Remove negative fees and invalid charges
   - Filter trips with unrealistic durations (<2min or >3hrs)
   - Eliminate duplicate records and generate trip_id

3. **⭐ PHASE 3: Star Schema Transformation**
   - Create dimension tables with surrogate keys
   - Generate fact table with foreign key relationships
   - Implement dimensional modeling best practices

4. **🔍 PHASE 4: Data Quality Validation**
   - Key validation and referential integrity checks
   - Data quality metrics and anomaly detection

5. **💾 PHASE 5: Data Storage**
   - Export to Parquet format (optimized for analytics)
   - Export to CSV format (human-readable backup)
   - Configurable output directory structure

## 🧪 Testing Infrastructure

- **Fixture-Based Testing**: Comprehensive test data generation
- **Component Coverage**: Individual tests for loader, cleaner, transformer, storer
- **Integration Testing**: End-to-end pipeline validation
- **Edge Case Handling**: Corrupted data, missing values, boundary conditions
- **Isolated Testing**: Temporary directories for test execution

## 📊 Key Features

### Data Quality & Validation
- Automated negative fee removal
- Trip duration validation (2 minutes - 3 hours)
- Duplicate record detection and removal
- Referential integrity validation

### Performance Optimizations
- Memory management with garbage collection
- Efficient column renaming and standardization
- Parquet format for analytical workloads
- Configurable batch processing

### Enterprise-Ready Logging
- Execution timing for performance monitoring
- Detailed phase-by-phase logging
- Error handling with failure duration tracking
- Professional log formatting with emojis for readability

## 🔧 Configuration

### Storage Configuration
The pipeline supports flexible storage configuration through the `base_dir` parameter in storer functions:

```python
# Local storage (default)
storer.store_to_parquet(trip_fact, vendor_dim, ...)

# Custom directory
storer.store_to_parquet(trip_fact, vendor_dim, ..., base_dir="custom/path")
```

### Logging Configuration
Centralized logging setup in `engine/logger_config.py` with:
- Execution timing decorators
- Configurable log levels
- Professional formatting standards

## 🚀 Future Enhancements

- **Cloud Deployment**: AWS S3/Azure Blob storage integration
- **Incremental Processing**: Delta/streaming data capabilities  
- **Data Lineage**: Apache Airflow workflow orchestration
- **Monitoring**: Prometheus metrics and Grafana dashboards
- **API Interface**: FastAPI endpoints for data access
- **Machine Learning**: Trip demand forecasting models

## 📈 Technical Stack

- **Data Processing**: pandas, numpy, pyarrow
- **Testing**: pytest with comprehensive fixtures
- **Geospatial**: geopandas, shapely for location analysis
- **Storage**: Parquet (analytics) + CSV (compatibility)
- **Logging**: Python logging with custom decorators
- **Environment**: Virtual environment with requirements.txt

## 🏗️ Architecture Principles

- **Separation of Concerns**: Each module has a single responsibility
- **Testability**: Fixture-based testing for all components
- **Configurability**: Flexible parameters for different environments
- **Observability**: Comprehensive logging and timing metrics
- **Scalability**: Modular design supporting future enhancements
- **Data Quality**: Built-in validation and cleaning operations

---

**🚖 Ready to process millions of NYC taxi trips with enterprise-grade reliability!**
