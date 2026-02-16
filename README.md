# Multi-Source Payer Loader (CSV & Manual Input)

## Project Overview

This project implements a Python-based data loading utility that:

- Extracts claim data from multiple sources (CSV files & Manual Input)
- Transforms the data with payer-specific business rules
- Adds ingestion metadata
- Loads data into payer-specific Snowflake target tables (simulated)

The solution demonstrates core Python concepts such as:

- Argument Parsing (`argparse`)
- Function Overloading Simulation (`*args`)
- Data Transformation using Pandas
- Object-Oriented Programming (Inheritance & Method Overriding)
- Layered ETL design approach

------------------------------------------------------------------------

## Architecture

Source (CSV / Manual Data)  
→ Extraction (Pandas)  
→ Transformation (Business Rules + Metadata)  
→ Loader Layer (Payer-Specific Mapping)  
→ Snowflake Target Tables (Simulated)

------------------------------------------------------------------------

## Tech Stack

- Python 3.x
- Pandas
- argparse
- datetime
- Object-Oriented Programming

------------------------------------------------------------------------

## Project Structure

MULTI_SOURCE_PAYER_LOADER/  
│  
├── payer_loader.py  
├── requirements.txt  
├── .gitignore  
├── README.md  
│  
└── data/  
  ├── anthem.csv  
  └── cigna.csv  

------------------------------------------------------------------------

## Setup Instructions

### 1. Create Virtual Environment (Optional but Recommended)

```bash
python -m venv venv
venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

If requirements.txt does not exist, install manually:

```bash
pip install pandas
```

------------------------------------------------------------------------

## Sample Input Files

### anthem.csv

### cigna.csv

### manual in code


Format of csv:

```
member_id,claim_id,claim_amount,service_date
4,2001,400,2025-02-01
5,2002,600,2025-02-02
```

------------------------------------------------------------------------

## Snowflake Target Tables (Simulated Mapping)

The loader maps payer names to Snowflake tables:

- anthem → SNOWFLAKE.RAW.ANTHEM_TABLE
- cigna → SNOWFLAKE.RAW.CIGNA_TABLE
- manual → SNOWFLAKE.RAW.GENERIC_CLAIMS_TABLE

(Note: Current implementation prints output instead of actual Snowflake load.)

------------------------------------------------------------------------

## Run the Application

### 1. Run for Anthem CSV

```bash
python payer_loader.py --payer anthem --source data/anthem.csv
```

### 2. Run for Cigna CSV

```bash
python payer_loader.py --payer cigna --source data/cigna.csv
```

### 3. Run for Manual Data (No File Required)

```bash
python payer_loader.py --payer manual
```

------------------------------------------------------------------------

## Data Transformations

### Common Transformations

- Adds `ingestion_timestamp`
- Adds `payer_name` column

### Anthem-Specific Logic

- Increases `claim_amount` by 5%

```python
df["claim_amount"] = df["claim_amount"] * 1.05
```

------------------------------------------------------------------------

## Execution Flow

1. `main()` function starts
2. Command-line arguments parsed using `parse_arguments()`
3. Data prepared using `prepare_dataframe()`
   - Reads CSV if file path provided
   - Converts manual list into DataFrame
4. Data transformed using `transform_data()`
5. `PayerLoader` object created
6. `load()` method executed
7. Data printed with record count and table name

## Validation Output (Console)

The loader prints:

Loading data into SNOWFLAKE.RAW.ANTHEM_TABLE  
Total records: X  

Data Preview:  
<transformed dataframe>  

Load completed successfully.

------------------------------------------------------------------------

## Performance Considerations

- Lightweight CLI-based utility
- Pandas used for fast in-memory transformations
- Easily extendable for real Snowflake bulk loading
- Scalable by adding more payer mappings

------------------------------------------------------------------------

## Future Enhancements

- Add real Snowflake connection
- Add logging instead of print statements
- Add exception handling and validation
- Convert to production-ready ETL pipeline
- Add unit tests

------------------------------------------------------------------------

## Author

KOTTAPALLI SRAVYA  
Data Engineering Project
