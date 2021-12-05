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

DIM_EQUITIES_INFO_TABLE_ID = "equities_info"
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
]

DIM_EQUITIES_INDICATORS_TABLE_ID = "equities_indicators"
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
        "name": "dividend_yield",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Last dividend.",
    },
    {
        "name": "shares_issued",
        "type": "FLOAT64",
        "mode": "REQUIRED",
        "description": "Number of shares at the specified date.",
    },
]

DIM_EQUITIES_FINANCE_TABLE_ID = "equities_finance"
DIM_EQUITIES_FINANCE_SCHEMA = [
    {
        "name": "date",
        "type": "DATE",
        "mode": "REQUIRED",
        "description": "Date when data was scraped.",
    },
    {
        "name": "isin_code",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "ISIN code for the equity.",
    },
    {
        "name": "period",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Period of the data.",
    },
    {
        "name": "revenues_from_sales",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Revenue from sales.",
    },
    {
        "name": "profit_loss_on_sales",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Profit/loss on sales.",
    },
    {
        "name": "operating_profit_loss",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Operating profit/loss.",
    },
    {
        "name": "financial_income",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Financial income.",
    },
    {
        "name": "profit_loss_before_tax",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Profit/loss before tax.",
    },
    {
        "name": "net_profit_loss_attributable_to_equity_holders_of_parent",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Net profit/loss attributable to equity holders of the parent.",
    },
    {
        "name": "depreciation",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Depreciation.",
    },
    {
        "name": "equity_shareholders_of_parent",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Equity shareholders of the parent.",
    },
    {
        "name": "assets",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Assets.",
    },
    {
        "name": "non_current_assets",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Non current assets.",
    },
    {
        "name": "current_assets",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Current assets.",
    },
    {
        "name": "current_ratio",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Current ratio.",
    },
    {
        "name": "quick_ratio",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Quick ratio.",
    },
    {
        "name": "debt_service_ratio",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Debt service ratio.",
    },
    {
        "name": "return_on_assets",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Return on assets.",
    },
    {
        "name": "return_on_equity",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Return on equity.",
    },
    {
        "name": "other_operating_revenues",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Other operating revenues.",
    },
    {
        "name": "share_capital",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Share capital.",
    },
    {
        "name": "non_current_liabilities",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Non current liabilities.",
    },
    {
        "name": "current_liabilities",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Current liabilities.",
    },
    {
        "name": "cash_flow_from_operating_activities",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Cash flow from operating activities.",
    },
    {
        "name": "cash_flow_from_investing_activities",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Cash flow from investing activities.",
    },
    {
        "name": "cash_flow_from_financing_activities",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Cash flow from financing activities.",
    },
    {
        "name": "purchase_of_property_plant_equipment_and_intangible_assets",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Purchase of property, plant, equipment and intangible assets.",
    },
    {
        "name": "net_cash_flow",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "Net cash flow.",
    },
    {
        "name": "ebitda",
        "type": "FLOAT64",
        "mode": "NULLABLE",
        "description": "EBITDA.",
    },
]

TABLES = {
    "equities": HISTORY_EQUITIES_TABLE_ID,
    "indices": HISTORY_INDICES_TABLE_ID,
    "info": DIM_EQUITIES_INFO_TABLE_ID,
    "indicators": DIM_EQUITIES_INDICATORS_TABLE_ID,
    "finance": DIM_EQUITIES_FINANCE_TABLE_ID,
}

SCHEMAS = {
    "equities": HISTORY_EQUITIES_SCHEMA,
    "indices": HISTORY_INDICES_SCHEMA,
    "info": DIM_EQUITIES_INFO_SCHEMA,
    "indicators": DIM_EQUITIES_INDICATORS_SCHEMA,
    "finance": DIM_EQUITIES_FINANCE_SCHEMA,
}
