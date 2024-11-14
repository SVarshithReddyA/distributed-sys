import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from werkzeug.utils import secure_filename

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
# Retrieve connection string from environment variable
connection_string = "DefaultEndpointsProtocol=https;AccountName=distributedsys;AccountKey=jLyjj/LyKtRVjlVfdawM5uaBhQZfBUiec+9xxiPxgWVXzn/yng4Rp6/XZojKSjVWZbejiBXuZ+MS+ASteWFJaA==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
UPLOAD_CONTAINER = "filestore"

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Access file content directly from the request body
        file = req.files.get("file") or req.get_body()
        if not file:
            return func.HttpResponse("No file provided", status_code=400)

            # Set a filename
        filename = secure_filename(req.route_params.get("file", "uploaded_file"))

            # Upload to blob
        blob_client = blob_service_client.get_blob_client(container=UPLOAD_CONTAINER, blob=filename)
        blob_client.upload_blob(file, overwrite=True)
           
        return func.HttpResponse(f"File '{filename}' uploaded successfully.", status_code=200)
    except Exception as e:
        logging.error(f"Error uploading file: {e}")
        return func.HttpResponse(f"Error uploading file: {str(e)}", status_code=500)


