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
