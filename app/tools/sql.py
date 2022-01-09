"""Contains functions for generarting sql queries."""
from typing import Optional


def replace_in_transaction(
    dataset_id: str, dest_table: str, src_table: str, delete_using: str
) -> str:
    """Creates a replace query for BigQuery where data from
    destination table is replaced from the data in temp table
    in a single transaction.
    """
    return f"""
        BEGIN
            BEGIN TRANSACTION;
            DELETE FROM `{dataset_id}.{dest_table}` WHERE {delete_using} IN (
                SELECT DISTINCT({delete_using}) FROM `{dataset_id}.{src_table}`
            );
            INSERT INTO `{dataset_id}.{dest_table}` SELECT * FROM `{dataset_id}.{src_table}`;
            COMMIT TRANSACTION;

        EXCEPTION WHEN ERROR THEN
            SELECT @@error.message;
            ROLLBACK TRANSACTION;
        END;
    """


def distinct(
    column: str,
    table_id: str,
    where: Optional[str] = None,
    groupby: Optional[str] = None,
):
    qry = f"""
        SELECT 
        CASE
            WHEN COUNT({column}) = COUNT(DISTINCT({column}))
            THEN true
            ELSE false
        END AS cmp
        FROM {table_id}
    """
    if where:
        qry += f" WHERE {where}"
    if groupby:
        qry += f" GROUP BY {groupby}"
    return qry


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
            SELECT COUNT({column}) AS cnt
            FROM {table_id} WHERE date = "{second_date}" GROUP BY date
        )
        SELECT
            IF((today.cnt/yesterday.cnt > {1.0 - margin}) AND (today.cnt/yesterday.cnt < {1.0 + margin}), TRUE, FALSE) as cmp
        FROM today CROSS JOIN yesterday;
    """
