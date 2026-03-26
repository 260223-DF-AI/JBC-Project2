# JBC Presents: Data Analysis - HTTP Style
This project demonstrates the ability to:
- Convert data in CSV format to a columnar format, ideal for analysis
- Upload a file to GCS via Python
- Initiate and retrieve data from a BigQuery SQL query
- Orchestrate these events via FastAPI http requests

## Running the project
From the top level folder, run
`py -m app`
to start the FastAPI server.

To view the webapp properly, host `index.html` on a server.
(Consider using VSCode extension `Live Server` for ease)

## Service Accounts

### Manual (Our approach)
- Go to Google Cloud Console
- Open IAM & Admin in the top-left hamburger menu
- Select "Service Accounts" in the resulting sub-menu
- Click "Create Service Account" at the top of the page
- Define the ID of the service account
    - Resulting "email" will be `id@project-id.iam.gserviceaccount.com`
- Define permissions and principals
- Download credentials JSON file
- Add file location to `.env`
- Load `.env` using dotenv module in Python standard library and connect to client for requests:

```python
    import dotenv
    import Path

    load_dotenv()
    creds_path = Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")).resolve()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(creds_path)

    from google.cloud import bigquery
    client = bigquery.Client()
    # OR
    from google.cloud import storage
    client = storage.Client()
```

### Python
[Setting up Service Accounts in GCP using Python](https://docs.cloud.google.com/iam/docs/service-accounts-create#iam-service-accounts-create-python)