from fastapi import FastAPI, HTTPException
from google.cloud import storage
from google.cloud.exceptions import NotFound

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/files/")
async def list_files():
    bucket_name = '2023-2-tarea3'

    try:
        # Authenticate using the service account key
        client = storage.Client.from_service_account_json('tarea3-service-key.json')

        # Get a reference to the bucket
        bucket = client.bucket(bucket_name)

        # List all files in the bucket
        files = bucket.list_blobs()

        # Extract file information
        file_list = [{'name': file.name, 'url': f'https://storage.googleapis.com/{bucket_name}/{file.name}'} for file in files]

        return {"files": file_list}

    except NotFound:
        raise HTTPException(status_code=404, detail="Bucket not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
