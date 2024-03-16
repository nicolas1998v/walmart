db_name='walmart.db'

sqlite3 $db_name '.read schema.sql' '.import --csv --skip 1 grocery_sales.csv grocery_sales'