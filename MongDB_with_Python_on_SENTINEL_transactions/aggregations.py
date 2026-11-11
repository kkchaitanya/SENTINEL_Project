"""
MongoDB Aggregation Pipelines for SENTINEL Transactions:
1. Fraud rate per merchant_category.
2. Top 10 customers by amount in their last 24 hours of activity.
"""

import asyncio
from pprint import pprint
import pandas as pd
from database import client, db


# ==============================================================================
# PIPELINE 1: Fraud Rate per Merchant Category
# ==============================================================================
# Computes the total transactions, fraudulent transactions, and fraud rate
# (both ratio and percentage) grouped by merchant_category.
#
# Supports:
# - Linked fraud labels in a separate 'labels' collection (via $lookup).
# - Embedded 'fraud_label' or 'is_fraud' fields directly on 'transactions'.
# ==============================================================================

FRAUD_RATE_PER_MERCHANT_CATEGORY_PIPELINE = [
    # 1. Look up fraud labels from 'labels' collection if present
    {
        "$lookup": {
            "from": "labels",
            "localField": "transaction_id",
            "foreignField": "transaction_id",
            "as": "label_info",
        }
    },
    # 2. Extract fraud indicator (from joined 'labels' or directly from transaction)
    {
        "$addFields": {
            "fraud_flag": {
                "$cond": [
                    {"$gt": [{"$size": "$label_info"}, 0]},
                    {"$arrayElemAt": ["$label_info.fraud_label", 0]},
                    {"$ifNull": ["$fraud_label", {"$ifNull": ["$is_fraud", 0]}]},
                ]
            }
        }
    },
    # 3. Group by merchant_category and count total vs fraud transactions
    {
        "$group": {
            "_id": "$merchant_category",
            "total_transactions": {"$sum": 1},
            "fraud_transactions": {
                "$sum": {
                    "$cond": [
                        {
                            "$or": [
                                {"$eq": ["$fraud_flag", 1]},
                                {"$eq": ["$fraud_flag", True]},
                                {"$eq": ["$fraud_flag", "1"]},
                                {"$eq": ["$fraud_flag", "true"]},
                            ]
                        },
                        1,
                        0,
                    ]
                }
            },
        }
    },
    # 4. Filter out any null/empty category names
    {
        "$match": {
            "_id": {"$ne": None}
        }
    },
    # 5. Compute fraud rate (ratio and percentage rounded to 2 decimals)
    {
        "$project": {
            "_id": 0,
            "merchant_category": "$_id",
            "total_transactions": 1,
            "fraud_transactions": 1,
            "fraud_rate": {
                "$cond": [
                    {"$gt": ["$total_transactions", 0]},
                    {"$divide": ["$fraud_transactions", "$total_transactions"]},
                    0.0,
                ]
            },
            "fraud_rate_pct": {
                "$cond": [
                    {"$gt": ["$total_transactions", 0]},
                    {
                        "$round": [
                            {
                                "$multiply": [
                                    {"$divide": ["$fraud_transactions", "$total_transactions"]},
                                    100,
                                ]
                            },
                            2,
                        ]
                    },
                    0.0,
                ]
            },
        }
    },
    # 6. Sort from highest fraud rate to lowest
    {
        "$sort": {
            "fraud_rate": -1,
            "total_transactions": -1,
        }
    },
]


# ==============================================================================
# PIPELINE 2: Top 10 Customers by Amount in Their Last 24 Hours of Activity
# ==============================================================================
# For each customer:
# 1. Finds their most recent transaction timestamp (their personal latest activity).
# 2. Computes their 24-hour activity window: [last_activity - 24 hours, last_activity].
# 3. Filters transactions that fell inside that window.
# 4. Aggregates the total amount spent within those 24 hours.
# 5. Enriches with customer metadata (city, age) from 'customers' collection.
# 6. Returns the top 10 customers sorted by highest amount in descending order.
# ==============================================================================

TOP_10_CUSTOMERS_LAST_24H_PIPELINE = [
    # 1. Standardize transaction date and amount in Rupees
    {
        "$addFields": {
            "txn_date": {"$toDate": "$transaction_time"},
            "amount": {
                "$toDouble": {
                    "$ifNull": [
                        "$transaction_amount",
                        {"$divide": ["$amount_paise", 100]},
                    ]
                }
            },
        }
    },
    # 2. Group by customer to find their personal latest transaction date
    #    and collect transaction records for filtering
    {
        "$group": {
            "_id": "$customer_id",
            "last_activity_time": {"$max": "$txn_date"},
            "transactions": {
                "$push": {
                    "transaction_id": "$transaction_id",
                    "amount": "$amount",
                    "txn_date": "$txn_date",
                }
            },
        }
    },
    # 3. Calculate 24-hour window start (last_activity_time - 86,400,000 milliseconds)
    {
        "$addFields": {
            "window_start_time": {
                "$subtract": ["$last_activity_time", 86400000]
            }
        }
    },
    # 4. Filter only transactions occurring within [last_activity - 24h, last_activity]
    {
        "$project": {
            "customer_id": "$_id",
            "last_activity_time": 1,
            "window_start_time": 1,
            "recent_transactions": {
                "$filter": {
                    "input": "$transactions",
                    "as": "item",
                    "cond": {
                        "$gte": ["$$item.txn_date", "$window_start_time"]
                    },
                }
            },
        }
    },
    # 5. Sum the amounts and count transactions inside that 24-hour window
    {
        "$project": {
            "_id": 0,
            "customer_id": 1,
            "last_activity_time": 1,
            "window_start_time": 1,
            "txns_in_last_24h": {"$size": "$recent_transactions"},
            "total_amount_last_24h": {
                "$round": [{"$sum": "$recent_transactions.amount"}, 2]
            },
        }
    },
    # 6. Sort descending by 24h spend amount to get top spenders
    {
        "$sort": {"total_amount_last_24h": -1}
    },
    # 7. Limit to top 10
    {
        "$limit": 10
    },
    # 8. Enrich with customer details from 'customers' collection
    {
        "$lookup": {
            "from": "customers",
            "localField": "customer_id",
            "foreignField": "customer_id",
            "as": "customer_profile",
        }
    },
    {
        "$addFields": {
            "profile": {"$arrayElemAt": ["$customer_profile", 0]}
        }
    },
    {
        "$project": {
            "customer_id": 1,
            "total_amount_last_24h": 1,
            "txns_in_last_24h": 1,
            "last_activity_time": 1,
            "window_start_time": 1,
            "home_city": "$profile.home_city",
            "customer_age": "$profile.customer_age",
            "region": "$profile.region",
        }
    },
]


# ==============================================================================
# ASYNC EXECUTION HELPERS
# ==============================================================================

async def get_fraud_rate_per_merchant_category(database=db):
    """
    Executes the fraud rate aggregation pipeline on the transactions collection.
    Returns a list of dictionaries with fraud rate metrics per merchant category.
    """
    cursor = database.transactions.aggregate(
        FRAUD_RATE_PER_MERCHANT_CATEGORY_PIPELINE,
        allowDiskUse=True
    )
    results = await cursor.to_list(length=None)
    return results


async def get_top_10_customers_last_24h(database=db):
    """
    Executes the top 10 customers aggregation pipeline on the transactions collection.
    Returns the top 10 customers by spend in their last 24h activity window.
    """
    cursor = database.transactions.aggregate(
        TOP_10_CUSTOMERS_LAST_24H_PIPELINE,
        allowDiskUse=True
    )
    results = await cursor.to_list(length=10)
    return results


# ==============================================================================
# MAIN TEST / CLI RUNNER
# ==============================================================================

async def main():
    print("=" * 75)
    print("1. RUNNING AGGREGATION: Fraud Rate per Merchant Category")
    print("=" * 75)

    try:
        fraud_data = await get_fraud_rate_per_merchant_category()
        if fraud_data:
            df_fraud = pd.DataFrame(fraud_data)
            print(df_fraud.to_string(index=False))
        else:
            print("No records found in transactions collection.")
    except Exception as e:
        print(f"Error executing Fraud Rate Pipeline: {e}")

    print("\n" + "=" * 75)
    print("2. RUNNING AGGREGATION: Top 10 Customers by Amount in Last 24 Hours")
    print("=" * 75)

    try:
        top_customers = await get_top_10_customers_last_24h()
        if top_customers:
            df_cust = pd.DataFrame(top_customers)
            print(df_cust.to_string(index=False))
        else:
            print("No records found in transactions collection.")
    except Exception as e:
        print(f"Error executing Top 10 Customers Pipeline: {e}")

    client.close()


if __name__ == "__main__":
    asyncio.run(main())

