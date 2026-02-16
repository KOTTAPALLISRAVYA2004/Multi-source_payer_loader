import argparse
import pandas as pd
from datetime import datetime


# --------------------------------------------------
# 1. Argument Parser
# --------------------------------------------------

def parse_arguments():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Multi-Source Payer Loader Utility"
    )

    parser.add_argument(
        "--source",
        type=str,
        help="Path to the CSV file"
    )

    parser.add_argument(
        "--payer",
        type=str,
        required=True,
        choices=["anthem", "cigna", "manual"],
        help="Name of the payer"
    )

    return parser.parse_args()

# 2. Simulated Function Overloading using *args

def prepare_dataframe(*args):
    """
    Prepare DataFrame based on input type.
    If input is string -> read CSV
    If input is list  -> convert to DataFrame
    """

    if not args:
        raise ValueError("No input data provided.")

    data = args[0]

    # Case 1: If input is file path (string)
    if isinstance(data, str):
        print("Reading data from CSV file...")
        return pd.read_csv(data)

    # Case 2: If input is list of dictionaries
    elif isinstance(data, list):
        print("Converting manual data to DataFrame...")
        return pd.DataFrame(data)

    else:
        raise TypeError(
            "Unsupported data type. Provide file path or list of dictionaries."
        )


# 3. Data Transformation

def transform_data(df: pd.DataFrame, payer: str) -> pd.DataFrame:
    """
    Perform basic transformations:
    - Add ingestion timestamp
    - Add payer name
    - Apply payer-specific logic
    """

    # Add ingestion timestamp
    df["ingestion_timestamp"] = datetime.now()

    # Add payer name column
    df["payer_name"] = payer

    # Special logic for Anthem
    if payer == "anthem":
        df["claim_amount"] = df["claim_amount"] * 1.05
        print("Applied Anthem-specific transformation")

    return df


# --------------------------------------------------
# 4. Base Loader Class
# --------------------------------------------------

class BaseLoader:
    """
    Base loader class.
    """

    def load(self, df: pd.DataFrame):
        raise NotImplementedError(
            "Subclasses must implement the load method."
        )


# --------------------------------------------------
# 5. Payer Loader Class (Inheritance + Overriding)
# --------------------------------------------------

class PayerLoader(BaseLoader):
    """
    Loader for specific payer tables.
    """

    def __init__(self, payer: str):
        self.payer = payer

    def load(self, df: pd.DataFrame):
        """
        Load data into payer-specific table.
        """

        table_mapping = {
            "anthem": "SNOWFLAKE.RAW.ANTHEM_TABLE",
            "cigna": "SNOWFLAKE.RAW.CIGNA_TABLE",
            "manual": "SNOWFLAKE.RAW.GENERIC_CLAIMS_TABLE"
        }

        target_table = table_mapping.get(self.payer)

        if not target_table:
            raise ValueError("Invalid payer specified.")

        print(f"\nLoading data into {target_table}")
        print(f"Total records: {len(df)}")
        print("\nData Preview:")
        print(df)
        print("Load completed successfully.\n")


# --------------------------------------------------
# 6. Main Execution Flow
# --------------------------------------------------

def main():
    """
    Main program execution.
    """

    args = parse_arguments()

    # Case: Manual data
    if args.payer == "manual":

        manual_data = [
            {
                "member_id": 7,
                "claim_id": 107,
                "claim_amount": 500,
                "service_date": "2025-01-07"
            },
            {
                "member_id": 8,
                "claim_id": 108,
                "claim_amount": 700,
                "service_date": "2025-01-08"
            }
        ]

        df = prepare_dataframe(manual_data)

    # Case: File-based data
    else:

        if not args.source:
            raise ValueError(
                "Source file must be provided for file-based loading."
            )

        df = prepare_dataframe(args.source)

    # Transform data
    df = transform_data(df, args.payer)

    # Load data
    loader = PayerLoader(args.payer)
    loader.load(df)


# 7. Entry Point

if __name__ == "__main__":
    main()