import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from werkzeug.utils import secure_filename
import os

# Initialize Function App
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Retrieve connection string from environment variable
connection_string = "DefaultEndpointsProtocol=https;AccountName=distributedsys;AccountKey=jLyjj/LyKtRVjlVfdawM5uaBhQZfBUiec+9xxiPxgWVXzn/yng4Rp6/XZojKSjVWZbejiBXuZ+MS+ASteWFJaA==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
UPLOAD_CONTAINER = "filestore"

# Define the HTTP trigger function
@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function for image upload processed a request.')

    try:
        # Extract file from the request
        image_file = req.files['file']
        file_name = image_file.filename

        # Upload to Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=UPLOAD_CONTAINER, blob=file_name)
        blob_client.upload_blob(image_file.stream, overwrite=True)

        return func.HttpResponse(
            f"Image '{file_name}' uploaded successfully to Blob Storage.",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error uploading image: {e}")
        return func.HttpResponse(
            f"Failed to upload image. Error: {str(e)}",
            status_code=500
        )