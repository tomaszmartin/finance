"""Contains config for GPW dags."""

GCP_CONN_ID = "google_cloud"
DATASET_ID = "gpw"
BUCKET_NAME = "stocks_dl"

HISTORY_EQUITIES_TABLE_ID = "equities"
HISTORY_EQUITIES_SCHEMA = [
    {"name": "date", "type": "DATE", "mode": "REQUIRED", "description": "Date."},
    {
        "name": "name",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Name of the equity.",
    },
    {
        "name": "isin_code",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "ISIN code for the equity.",
    },
    {
        "name": "base",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Base currency for the price (in what currency the prices are).",
    },
    {
        "name": "opening_price",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Opening price.",
    },
    {
        "name": "closing_price",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Closing price.",
    },
    {
        "name": "minimum_price",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Minimum price.",
    },
    {
        "name": "maximum_price",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Maximum price.",
    },
    {
        "name": "number_of_transactions",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Number of transactions.",
    },
    {
        "name": "trade_volume",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Trade volume.",
    },
    {
        "name": "turnover_value",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Turnover value.",
    },
]

HISTORY_INDICES_TABLE_ID = "indices"
HISTORY_INDICES_SCHEMA = HISTORY_EQUITIES_SCHEMA

DIM_EQUITIES_INFO_TABLE_ID = "dim_equities_info"
DIM_EQUITIES_INFO_SCHEMA = [
    {"name": "date", "type": "DATE", "mode": "REQUIRED", "description": "Date."},
    {
        "name": "isin_code",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "ISIN code for the equity.",
    },
    {
        "name": "abbreviation",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Abbreviation of the company.",
    },
    {
        "name": "name",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Name of the company.",
    },
    {
        "name": "full_name",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Full name of the company.",
    },
    {
        "name": "address",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Company's address.",
    },
    {
        "name": "voivodeship",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Voivodeship in which company resides.",
    },
    {
        "name": "email",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Company's email address.",
    },
    {
        "name": "fax",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Company's fax number.",
    },
    {
        "name": "phone",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Company's phone number.",
    },
    {
        "name": "www",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Company's website address.",
    },
    {
        "name": "ceo",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Company's CEO.",
    },
    {"name": "regs", "type": "STRING", "mode": "NULLABLE", "description": ""},
    {
        "name": "first_listing",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Company's first listing.",
    },
    {
        "name": "market_value",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Company's market value at the specified date.",
    },
    {
        "name": "number_of_shares_issued",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Number of shares at the specified date.",
    },
]

DIM_EQUITIES_INDICATORS_TABLE_ID = "dim_equities_indicators"
DIM_EQUITIES_INDICATORS_SCHEMA = [
    {"name": "date", "type": "DATE", "mode": "REQUIRED", "description": "Date."},
    {
        "name": "isin_code",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "ISIN code for the equity.",
    },
    {
        "name": "market",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Market/Segment to which the company belongs to.",
    },
    {
        "name": "sector",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Sector to which the company belongs to.",
    },
    {
        "name": "book_value",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Company's book value at the specified date.",
    },
    {
        "name": "dividend_yield",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Last dividend.",
    },
    {
        "name": "market_value",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Company's market value at the specified date.",
    },
    {
        "name": "number_of_shares_issued",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Number of shares at the specified date.",
    },
    {
        "name": "pbv",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Price to book value ratio at the specified date.",
    },
    {
        "name": "pe",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Price to earnings ratio at the specified date.",
    },
]

TABLES = {
    "equities": HISTORY_EQUITIES_TABLE_ID,
    "indices": HISTORY_INDICES_TABLE_ID,
    "info": DIM_EQUITIES_INFO_TABLE_ID,
    "indicators": DIM_EQUITIES_INDICATORS_TABLE_ID,
}

SCHEMAS = {
    "equities": HISTORY_EQUITIES_SCHEMA,
    "indices": HISTORY_INDICES_SCHEMA,
    "info": DIM_EQUITIES_INFO_SCHEMA,
    "indicators": DIM_EQUITIES_INDICATORS_SCHEMA,
}