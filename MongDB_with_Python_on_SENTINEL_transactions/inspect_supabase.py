import os
import pandas as pd

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(
    SUPABASE_URL,
    SUPABASE_KEY
)

# Tables to export
TABLES = [
    "customers",
    "transactions",
    "devices",
    "merchants"
]

# Create output directory
os.makedirs("data", exist_ok=True)


def export_table(table_name):

    print(f"\nExporting {table_name}...")

    all_records = []
    batch_size = 1000
    start = 0

    while True:

        response = (
            supabase
            .table(table_name)
            .select("*")
            .range(start, start + batch_size - 1)
            .execute()
        )

        records = response.data

        if not records:
            break

        all_records.extend(records)

        print("Fetched:", len(all_records))

        if len(records) < batch_size:
            break

        start += batch_size

    if all_records:

        df = pd.DataFrame(all_records)

        filename = f"data/{table_name}.csv"

        df.to_csv(
            filename,
            index=False,
            encoding="utf-8"
        )

        print(f"Saved: {filename}")
        print("Total records:", len(df))

    else:
        print(f"No accessible records found in {table_name}")


# Export all tables
for table in TABLES:

    try:
        export_table(table)

    except Exception as e:
        print(f"Error exporting {table}: {e}")


print("\nCSV EXPORT COMPLETED!")