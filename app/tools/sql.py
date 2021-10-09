"""Contains functions for generarting sql queries."""


def replace_from_temp(
    dataset_id: str, dest_table: str, temp_table: str, delete_using: str
) -> str:
    """Creates a replace query for BigQuery where data from
    destination table is replaced from the data in temp table
    in a single transaction.
    """
    return f"""
        BEGIN
            BEGIN TRANSACTION;
            DELETE FROM {dataset_id}.{dest_table} WHERE {delete_using} IN (
                SELECT DISTINCT({delete_using}) FROM {dataset_id}.{temp_table}
            );
            INSERT INTO {dataset_id}.{dest_table} SELECT * FROM {dataset_id}.{temp_table};
            COMMIT TRANSACTION;

        EXCEPTION WHEN ERROR THEN
            SELECT @@error.message;
            ROLLBACK TRANSACTION;
        END;
    """


def distinct(
    column: str,
    table_id: str,
    since: str = "{{ execution_date.subtract(days=30).date() }}",
    date_col: str = "date",
):
    return f"""
        SELECT IF(COUNT({column})=COUNT(DISTINCT({column})), TRUE, FALSE) AS cmp
        FROM {table_id} 
        WHERE {date_col} >= '{since}' GROUP BY {date_col};
    """


def count_in_time(
    column: str,
    table_id: str,
    first_date: str = "{{ ds }}",
    second_date: str = "{{ prev_ds }}",
    margin: float = 0.1,
):
    return f"""
        WITH today AS (
            SELECT COUNT({column}) AS cnt
            FROM {table_id} WHERE date = "{first_date}" GROUP BY date
        ), yesterday AS (
            SELECT COUNT(isin_code) AS cnt
            FROM {table_id} WHERE date = "{second_date}" GROUP BY date
        )
        SELECT
            IF((today.cnt/yesterday.cnt > {1.0 - margin}) AND (today.cnt/yesterday.cnt < {1.0 + margin}), TRUE, FALSE) as cmp
        FROM today CROSS JOIN yesterday;
    """
