
# Solution Documentation

## Setup

### Prerequisites
- Python 3.12.2
- `venv` module

### Steps

1. **Download and unzip the solution files.**

2. **Create a virtual environment:**

    ```bash
    python3.12 -m venv venv
    ```

3. **Activate the virtual environment:**

     ```bash
     source venv/bin/activate
     ```


## Running the Solution

1. **Execute the main script:**

    ```bash
    python src/main.py
    ```
   
2. **Running tests**
   
   ```bash
   python -m unittest discover tests
   ```

## Verifying the Output

1. **Check the logs:**

    The script uses logging to provide information about its progress. Ensure you see messages indicating successful parsing, grouping, joining, and storing of records.

2. **Inspect the database:**

    The final SQLite database should contain 529 records in the metrics table. You can verify this using an SQLite browser or command line:

    ```bash
    sqlite3 <path_to_database>
    ```

    Replace `<path_to_database>` with the path to your SQLite database. Then, in the SQLite shell, run:

    ```sql
    SELECT COUNT(*) FROM metrics;
    ```

    Ensure the count is 529.

## Explanation of the Solution

1. **Extracting WAL Records:**
    
    The function `extract_wal_records_mapping` reads the WAL file and extracts records that represent inserts. It organizes these records by their respective tables.

2. **Processing WAL Records:**

    The function `process_wal_records_mapping` takes the extracted records and performs joins based on `flow_id`, `transaction_id`, and `token_id`. This creates a comprehensive record for each event.

3. **Persisting Resulted Metrics:**

    The function `persist_metrics_data` create a table if not exists and stores the metrics into an SQLite database.
4. **Cleaning database**
   
   With the current implementation, it will push those 529 records on every run. If you run multiple times, you may see more records.
   For the sake of challenge, things were kept simple for a single time run.
   If you want to run multiple times the implementation, you would have to delete the db file before running again.
   

## Conclusion

This solution effectively parses, processes, and stores WAL records into a structured SQLite database, ensuring all necessary data is captured and organized.
