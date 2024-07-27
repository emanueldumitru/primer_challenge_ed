# Standard library usage
import sys
from typing import List, Dict
import logging
import json
import sqlite3

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Setting up some constants
FIRST_ITEM = 0
SECOND_ITEM = 1
EVENT_DATA_TABLE = "event_v2_data"
TRANSACTION_TABLE = "transaction"
TRANSACTION_REQUEST_TABLE = "transaction_request"
PAYMENT_INSTRUMENT_TOKEN_DATA_TABLE = "payment_instrument_token_data"
COLUMN_NAMES = "columnnames"
COLUMN_VALUES = "columnvalues"
COLUMN_TYPES = "columntypes"
TRANSACTION_ID = "transaction_id"
FLOW_ID = "flow_id"
TOKEN_ID = "token_id"


# Extracting input
def extract_wal_records_mapping(wal_file_path: str) -> Dict:
    """
    :param wal_file_path: the path to the wal file to be parsed
    note : in real life scneario I might need to batch read the json file
    and process on batches to not overload the memory and treat the entire problem differently,
    but for the sake of challenge and input size, we will keep things simple
    :return: a dictionary containing a list of records per table
    """
    try:
        logger.info(f"Parsing WAL records file {wal_file_path}...")
        with open(wal_file_path, "r") as wal_file:
            wal_data_records = json.load(wal_file)

        insert_wal_data_records = [
            record["change"][FIRST_ITEM] for record in wal_data_records
            if any(changed_record["kind"] == "insert" for changed_record in record["change"])
        ]

        logger.info("Grouping WAL records per table...")
        insert_wal_data_records_mapping = {}
        for record in insert_wal_data_records:
            table = record["table"]
            record_object = {}

            logger.debug(f"Organizing WAL records object for {table}...")
            for index, column_name in enumerate(record[COLUMN_NAMES]):
                type = record[COLUMN_TYPES][index]
                value = record[COLUMN_VALUES][index]
                record_object[f"{table}.{column_name}"] = (value, type)

            if table not in insert_wal_data_records_mapping:
                insert_wal_data_records_mapping[table] = [record_object]
            else:
                insert_wal_data_records_mapping[table].append(record_object)

        return insert_wal_data_records_mapping
    except FileNotFoundError:
        logger.error(f"Error: The file {wal_file_path} was not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.error(f"Error: The file {wal_file_path} contains invalid JSON.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)


# Processing input to gather metrics
def process_wal_records_mapping(insert_wal_data_records_mapping: Dict) -> Dict:
    """
    :param insert_wal_data_records_mapping: a dictionary containing a list of records per table
    :return: A dictionary containing a list of metrics and mapping types
    """
    try:
        metrics = []

        events_data = insert_wal_data_records_mapping[EVENT_DATA_TABLE]
        transaction_data = insert_wal_data_records_mapping[TRANSACTION_TABLE]
        transaction_request_data = insert_wal_data_records_mapping[TRANSACTION_REQUEST_TABLE]
        payment_instrument_token_data = insert_wal_data_records_mapping[PAYMENT_INSTRUMENT_TOKEN_DATA_TABLE]

        logger.info(f"Joining {EVENT_DATA_TABLE} with {TRANSACTION_TABLE}...")
        events_transaction_data = \
            join_datasets([events_data, transaction_data],
                          [EVENT_DATA_TABLE, TRANSACTION_TABLE], TRANSACTION_ID)

        logger.info(f"Joining {EVENT_DATA_TABLE} with {TRANSACTION_REQUEST_TABLE}...")
        events_transaction_transaction_request_data = (
            join_datasets([events_transaction_data, transaction_request_data],
                          [EVENT_DATA_TABLE, TRANSACTION_REQUEST_TABLE], FLOW_ID))

        logger.info(f"Joining {TRANSACTION_REQUEST_TABLE} with {PAYMENT_INSTRUMENT_TOKEN_DATA_TABLE}...")
        events_transaction_transaction_request_payment_instrument_token_data = (
            join_datasets([events_transaction_transaction_request_data, payment_instrument_token_data],
                          [TRANSACTION_REQUEST_TABLE, PAYMENT_INSTRUMENT_TOKEN_DATA_TABLE], TOKEN_ID))

        logger.info("Preparing metrics mappings - records to store and their types ...")
        types = []
        for item in events_transaction_transaction_request_payment_instrument_token_data:
            decline_reason = None if not item[f"{EVENT_DATA_TABLE}.error_details"][FIRST_ITEM] else \
                convert_to_json_or_string(item[f"{EVENT_DATA_TABLE}.error_details"][FIRST_ITEM])["decline_reason"]
            decline_type = None if not item[f"{EVENT_DATA_TABLE}.error_details"][FIRST_ITEM] else \
                convert_to_json_or_string(item[f"{EVENT_DATA_TABLE}.error_details"][FIRST_ITEM])["decline_type"]
            payment_method = None if not item[f"{TRANSACTION_REQUEST_TABLE}.vault_options"][FIRST_ITEM] else \
                convert_to_json_or_string(item[f"{TRANSACTION_REQUEST_TABLE}.vault_options"][FIRST_ITEM])[
                    "payment_method"]
            customer_id = None if not item[f"{PAYMENT_INSTRUMENT_TOKEN_DATA_TABLE}.vault_data"][FIRST_ITEM] else \
                convert_to_json_or_string(item[f"{PAYMENT_INSTRUMENT_TOKEN_DATA_TABLE}.vault_data"][FIRST_ITEM])[
                    "customer_id"]

            metrics.append({
                "event_id": item[f"{EVENT_DATA_TABLE}.event_id"][FIRST_ITEM],
                "flow_id": item[f"{EVENT_DATA_TABLE}.flow_id"][FIRST_ITEM],
                "created_at": item[f"{EVENT_DATA_TABLE}.created_at"][FIRST_ITEM],
                "transaction_lifecycle_event": item[f"{EVENT_DATA_TABLE}.transaction_lifecycle_event"][FIRST_ITEM],
                "decline_reason": decline_reason,
                "decline_type": decline_type,
                "payment_method": payment_method,
                "transaction_id": item[f"{TRANSACTION_TABLE}.transaction_id"][FIRST_ITEM],
                "transaction_type": item[f"{TRANSACTION_TABLE}.transaction_type"][FIRST_ITEM],
                "amount": item[f"{TRANSACTION_TABLE}.amount"][FIRST_ITEM],
                "currency_code": item[f"{TRANSACTION_TABLE}.currency_code"][FIRST_ITEM],
                "processor_merchant_account_id": item[f"{TRANSACTION_TABLE}.processor_merchant_account_id"][FIRST_ITEM],
                "three_d_secure_authentication":
                    item[f"{PAYMENT_INSTRUMENT_TOKEN_DATA_TABLE}.three_d_secure_authentication"][FIRST_ITEM],
                "payment_instrument_type": item[f"{PAYMENT_INSTRUMENT_TOKEN_DATA_TABLE}.payment_instrument_type"][
                    FIRST_ITEM],
                "customer_id": customer_id
            })

            if not types:
                types = [
                    item[f"{EVENT_DATA_TABLE}.event_id"][SECOND_ITEM],
                    item[f"{EVENT_DATA_TABLE}.flow_id"][SECOND_ITEM],
                    item[f"{EVENT_DATA_TABLE}.created_at"][SECOND_ITEM],
                    item[f"{EVENT_DATA_TABLE}.transaction_lifecycle_event"][SECOND_ITEM],
                    "TEXT",  #item[f"{EVENT_DATA_TABLE}.error_details.decline_reason"][SECOND_ITEM],
                    "TEXT",  #item[f"{EVENT_DATA_TABLE}.error_details.decline_type"][SECOND_ITEM],
                    "TEXT",  #item[f"{TRANSACTION_REQUEST_TABLE}.vault_options.payment_method"][SECOND_ITEM],
                    item[f"{TRANSACTION_TABLE}.transaction_id"][SECOND_ITEM],
                    item[f"{TRANSACTION_TABLE}.transaction_type"][SECOND_ITEM],
                    item[f"{TRANSACTION_TABLE}.amount"][SECOND_ITEM],
                    item[f"{TRANSACTION_TABLE}.currency_code"][SECOND_ITEM],
                    item[f"{TRANSACTION_TABLE}.processor_merchant_account_id"][SECOND_ITEM],
                    item[f"{PAYMENT_INSTRUMENT_TOKEN_DATA_TABLE}.three_d_secure_authentication"][SECOND_ITEM],
                    item[f"{PAYMENT_INSTRUMENT_TOKEN_DATA_TABLE}.payment_instrument_type"][SECOND_ITEM],
                    "TEXT",  #item[f"{PAYMENT_INSTRUMENT_TOKEN_DATA_TABLE}.vault_data"][SECOND_ITEM]
                ]

        return {
            "data": metrics,
            "types": types
        }
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    return {}


def join_datasets(datasets, datasets_table_names, joining_column) -> List[Dict]:
    """
    :param datasets: the datasets to be joined
    :param datasets_table_names: the names of the datasets tables to be joined
    :param joining_column: joining column to be used in the joined datasets
    :return: joined dataset of objects including all columns collected
    """
    joined_data = []
    for dataset1_item in datasets[FIRST_ITEM]:
        for dataset2_item in datasets[SECOND_ITEM]:
            join_column_1 = f"{datasets_table_names[FIRST_ITEM]}.{joining_column}"
            join_column_2 = f"{datasets_table_names[SECOND_ITEM]}.{joining_column}"
            if dataset1_item[join_column_1][FIRST_ITEM] == dataset2_item[join_column_2][FIRST_ITEM]:  # (value,type)
                record = {**dataset1_item, **dataset2_item}
                joined_data.append(record)

    return joined_data


def convert_to_json_or_string(value):
    """
    :param value: the value to be converted
    :return: eiter a json python dictionary or a string
    """
    try:
        # Attempt to parse the string as JSON
        json_object = json.loads(value)
        return json_object
    except json.JSONDecodeError:
        # If parsing fails, return the original string
        return value


# Store output
def persist_metrics_data(metrics: Dict, db_file_path: str) -> None:
    """

    :param metrics Metrics mapping to be persisted, list of records and types
    :return:
    """
    # Insert metrics into the database
    conn = sqlite3.connect(db_file_path)
    try:
        cursor = conn.cursor()
        create_metric_table(cursor, metrics["types"])

        # Start a transaction
        conn.execute('BEGIN TRANSACTION')

        # Insert metrics
        insert_metrics_data(cursor, metrics["data"])

        # Commit the transaction
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"An error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()


def create_metric_table(cursor, types: List) -> None:
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS metrics (
        event_id {types[0]},
        flow_id {types[1]},
        created_at {types[2]},
        transaction_lifecycle_event {types[3]},
        decline_reason {types[4]},
        decline_type {types[5]},
        payment_method {types[6]},
        transaction_id {types[7]},
        transaction_type {types[8]},
        amount {types[9]},
        currency_code {types[10]},
        processor_merchant_account_id {types[11]},
        three_d_secure_authentication {types[12]},
        payment_instrument_type {types[13]},
        customer_id {types[14]}
    )
    """
    cursor.execute(create_table_query)


def insert_metrics_data(cursor, metrics: List[Dict]) -> None:
    insert_query = """
       INSERT INTO metrics (
           event_id, flow_id, created_at, transaction_lifecycle_event, 
           decline_reason, decline_type, payment_method, transaction_id, 
           transaction_type, amount, currency_code, processor_merchant_account_id, 
           three_d_secure_authentication, payment_instrument_type, customer_id
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       """
    for metric in metrics:
        cursor.execute(insert_query, (
            metric["event_id"], metric["flow_id"], metric["created_at"],
            metric["transaction_lifecycle_event"], metric["decline_reason"],
            metric["decline_type"], metric["payment_method"], metric["transaction_id"],
            metric["transaction_type"], metric["amount"], metric["currency_code"],
            metric["processor_merchant_account_id"], metric["three_d_secure_authentication"],
            metric["payment_instrument_type"], metric["customer_id"]
        ))


def main():
    wal_file_path = "wal.json"

    # Extract wal records mapping
    wal_records_mapping = extract_wal_records_mapping(wal_file_path)

    # Process metrics from the wal records mapping, return metrics and types
    metrics_mapping = process_wal_records_mapping(wal_records_mapping)

    # Persist metrics mapping to datasource
    logger.info("Persisting metrics data to db")
    persist_metrics_data(metrics_mapping, db_file_path = "metrics_datasource.db")


if __name__ == "__main__":
    main()
