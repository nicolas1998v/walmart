"""Schema definitions for Walmart pipeline data processing.

This module provides schema configurations and header definitions for both
clean and aggregated data formats used in the Walmart pipeline processing.

Exports:
    CLEAN_SCHEMA: Schema definition for cleaned data
    AGG_SCHEMA: Schema definition for aggregated data
    CLEAN_CSV_HEADERS: Headers for clean CSV files
    AGG_CSV_HEADERS: Headers for aggregated CSV files
"""

CLEAN_SCHEMA = {
    'fields': [
        {'name': 'Store_ID', 'type': 'INTEGER'},
        {'name': 'Month', 'type': 'INTEGER'},
        {'name': 'Dept', 'type': 'INTEGER'},
        {'name': 'IsHoliday', 'type': 'INTEGER'},
        {'name': 'Weekly_Sales', 'type': 'FLOAT'},
        {'name': 'CPI', 'type': 'FLOAT'},
        {'name': 'Unemployment', 'type': 'FLOAT'}
    ]
}

AGG_SCHEMA = {
    'fields': [
        {'name': 'Month', 'type': 'INTEGER'},
        {'name': 'Average_Weekly_Sales', 'type': 'FLOAT'}
    ]
}

CLEAN_CSV_HEADERS = 'index,Store_ID,Month,Dept,IsHoliday,Weekly_Sales,CPI,Unemployment'
AGG_CSV_HEADERS = 'index,Month,Average_Weekly_Sales'