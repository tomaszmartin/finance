"""Contains functions for generarting sql queries."""


def replace_from_temp_bigquery(
    dataset: str, dest_table: str, temp_table: str, delete_using: str
) -> str:
    """Creates a replace query for BigQuery where data from
    destination table is replaced from the data in temp table
    in a single transaction.
    """
    return f"""
        BEGIN
            BEGIN TRANSACTION;
            DELETE FROM {dataset}.{dest_table} WHERE {delete_using} IN (
                SELECT DISTINCT({delete_using}) FROM {dataset}.{temp_table}
            );
            INSERT INTO {dataset}.{dest_table} SELECT * FROM {dataset}.{temp_table};
            COMMIT TRANSACTION;

        EXCEPTION WHEN ERROR THEN
            SELECT @@error.message;
            ROLLBACK TRANSACTION;
        END;
    """
