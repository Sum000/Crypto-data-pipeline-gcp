from google.cloud import storage


BUCKET_NAME = "crypto-pipeline-project-bbbd1cf0-de1e-476f-af1"


def test_gcs_connection():
    client = storage.Client()

    bucket = client.get_bucket(BUCKET_NAME)

    print(f"Connected to bucket: {bucket.name}")


if __name__ == "__main__":
    test_gcs_connection()