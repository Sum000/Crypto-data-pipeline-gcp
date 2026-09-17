from google.cloud import storage


BUCKET_NAME = "crypto-pipeline-project-bbbd1cf0-de1e-476f-af1"

def upload_file_to_gcs(local_file_path, gcs_object_name):
    client = storage.Client()

    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_object_name)

    blob.upload_from_filename(local_file_path)

    gcs_uri = f"gs://{BUCKET_NAME}/{gcs_object_name}"

    print(f"Uploaded to: {gcs_uri}")

    return gcs_uri

def download_file_from_gcs(gcs_object_name, local_file_path):
    client = storage.Client()

    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_object_name)

    blob.download_to_filename(local_file_path)

    print(
        f"Downloaded from: "
        f"gs://{BUCKET_NAME}/{gcs_object_name}"
    )

def get_latest_raw_object():
    client = storage.Client()

    bucket = client.bucket(BUCKET_NAME)

    blobs = list(bucket.list_blobs(prefix="raw/crypto/"))

    if not blobs:
        raise FileNotFoundError("No raw crypto data found in GCS.")

    latest_blob = max(
        blobs,
        key=lambda blob: blob.updated
    )

    return latest_blob.name