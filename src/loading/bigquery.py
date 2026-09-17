from google.cloud import bigquery


PROJECT_ID = "project-bbbd1cf0-de1e-476f-af1"
DATASET_ID = "crypto_data"
TABLE_ID = "crypto_market_snapshot"

TABLE_REFERENCE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def load_csv_from_gcs(gcs_uri):
    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    print(f"Loading file from GCS: {gcs_uri}")
    print(f"Destination table: {TABLE_REFERENCE}")

    load_job = client.load_table_from_uri(
        gcs_uri,
        TABLE_REFERENCE,
        job_config=job_config,
    )

    load_job.result()

    table = client.get_table(TABLE_REFERENCE)

    print("BigQuery load completed successfully.")
    print(f"Current table row count: {table.num_rows}")


if __name__ == "__main__":
    print(
        "This module contains the BigQuery loading function.\n"
        "It is intended to receive a GCS URI from the pipeline."
    )