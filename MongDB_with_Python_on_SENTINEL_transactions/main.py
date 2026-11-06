import pandas as pd
import json
import asyncio
from database import (
    client,
    db,
    customers,
    transactions,
    devices,
    merchants
)
from config import settings

print(f"DB connection {settings.MONGODB_URI} ")

# ---------------------------------
# 1. LOAD CSV FILES
# ---------------------------------

customers_df = pd.read_csv("data/customers.csv")
transactions_df = pd.read_csv("data/transactions.csv")
devices_df= pd.read_csv("data/devices.csv")
merchants_df= pd.read_csv("data/merchants.csv")

print("Customers loaded:", len(customers_df))
print("Transactions loaded:", len(transactions_df))
print("devices loaded:", len(devices_df))
print("merchants loaded:", len(merchants_df))

# ---------------------------------
# 2. CLEAN DATA
# ---------------------------------

def clean_records(df):
    df = df.astype(object).where(pd.notna(df), None)
    records = df.to_dict("records")
    return records


customers_collection = clean_records(customers_df)
transactions_collection = clean_records(transactions_df)
devices_collection = clean_records(devices_df)
merchants_collection = clean_records(merchants_df)


# ---------------------------------
# 3. INSERT / REINSERT COLLECTIONS (WITH BATCHING & RETRIES)
# ---------------------------------
async def insert_in_batches(collection, records, collection_name, batch_size=2500, max_retries=3):
    total = len(records)
    if total == 0:
        print(f"No records to insert for '{collection_name}'.")
        return

    print(f"\nInserting {total} records into '{collection_name}' (batches of {batch_size})...")
    inserted_count = 0

    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        for attempt in range(1, max_retries + 1):
            try:
                result = await collection.insert_many(batch, ordered=False)
                inserted_count += len(result.inserted_ids)
                percent = (inserted_count / total) * 100
                print(f"  [{collection_name}] {inserted_count}/{total} records inserted ({percent:.1f}%)")
                break
            except Exception as e:
                print(f"  [Attempt {attempt}/{max_retries}] Batch {i}-{i+len(batch)} failed in '{collection_name}': {e}")
                if attempt < max_retries:
                    await asyncio.sleep(2 * attempt)
                else:
                    raise e


async def insert_collection():
    try:
        # Ping MongoDB cluster to ensure connection is active
        print("Pinging MongoDB cluster...")
        await client.admin.command("ping")
        print("Connected to MongoDB successfully!\n")

        existing_collections = await db.list_collection_names()
        print(f"Existing collections in DB: {existing_collections}")

        collections_to_load = [
            ("customers", customers, customers_collection),
            ("devices", devices, devices_collection),
            ("merchants", merchants, merchants_collection),
            ("transactions", transactions, transactions_collection),
        ]

        for name, collection, records in collections_to_load:
            if name in existing_collections:
                print(f"\nCollection '{name}' already exists. Deleting (dropping) collection...")
                try:
                    await collection.drop()
                    print(f"Collection '{name}' deleted.")
                except Exception as drop_err:
                    print(f"Warning dropping '{name}': {drop_err}. Clearing via delete_many...")
                    await collection.delete_many({})

            await insert_in_batches(collection, records, name, batch_size=2500)

        print("\nAll collections processed and inserted successfully!")
    except Exception as ex:
        print(f"\nError during collection processing: {ex}")
    finally:
        client.close()

if __name__ == "__main__":
    asyncio.run(insert_collection())

