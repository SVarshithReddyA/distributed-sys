import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from werkzeug.utils import secure_filename
import os

# Initialize the Azure Function app
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Set up the Blob service client
connection_string = "DefaultEndpointsProtocol=https;AccountName=distributedsys;AccountKey=jLyjj/LyKtRVjlVfdawM5uaBhQZfBUiec+9xxiPxgWVXzn/yng4Rp6/XZojKSjVWZbejiBXuZ+MS+ASteWFJaA==;EndpointSuffix=core.windows.net"  # Use environment variable
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
UPLOAD_CONTAINER = "filestore"  # Your target blob container name

# Ensure the container exists or create it if it doesn't
try:
    container_client = blob_service_client.get_container_client(UPLOAD_CONTAINER)
    container_client.get_container_properties()
except Exception:
    container_client = blob_service_client.create_container(UPLOAD_CONTAINER)


def upload_file_to_blob(req: func.HttpRequest) -> func.HttpResponse:
    """
    Helper function to handle file upload.
    """
    try:
        # Access file content directly from the request body
        file = req.files.get("file") or req.get_body()
        if not file:
            return func.HttpResponse("No file provided", status_code=400)

        # Set a filename
        filename = secure_filename(req.params.get("file", "uploaded_file"))

        # Upload to blob
        blob_client = blob_service_client.get_blob_client(container=UPLOAD_CONTAINER, blob=filename)
        blob_client.upload_blob(file, overwrite=True)

        return func.HttpResponse(f"File '{filename}' uploaded successfully.", status_code=200)
    except Exception as e:
        logging.error(f"Error uploading file: {e}")
        return func.HttpResponse(f"Error uploading file: {str(e)}", status_code=500)


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger function that either responds with a greeting or handles file uploads.
    """
    logging.info('Python HTTP trigger function processed a request.')

    # Check the route or query parameter to determine the action
    action = req.params.get('action')
    
    if action == "upload":
        # Delegate to the file upload helper function
        return upload_file_to_blob(req)

    # Default behavior: Greeting functionality
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
        )
