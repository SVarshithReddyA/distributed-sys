import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from werkzeug.utils import secure_filename
import pandas as pd
from io import BytesIO

# Initialize the Azure Function app
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Blob storage configuration
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=distributedsys;AccountKey=jLyjj/LyKtRVjlVfdawM5uaBhQZfBUiec+9xxiPxgWVXzn/yng4Rp6/XZojKSjVWZbejiBXuZ+MS+ASteWFJaA==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
UPLOAD_CONTAINER = "filestore"

# Ensure the container exists
try:
    container_client = blob_service_client.get_container_client(UPLOAD_CONTAINER)
    container_client.get_container_properties()
except Exception:
    container_client = blob_service_client.create_container(UPLOAD_CONTAINER)

# Upload HTTP-triggered function
def upload_file_to_blob(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Access file content directly from the request
        file = req.files.get("file")
        if not file:
            return func.HttpResponse("No file provided", status_code=400)

        # Derive filename
        filename = secure_filename(file.filename) if hasattr(file, "filename") else "uploaded_file"

        # Use 'uploads/' prefix to organize blobs
        blob_name = f"uploads/{filename}"
        blob_client = blob_service_client.get_blob_client(container=UPLOAD_CONTAINER, blob=blob_name)
        blob_client.upload_blob(file.read(), overwrite=True)

        return func.HttpResponse(f"File '{filename}' uploaded successfully to 'uploads/'.", status_code=200)
    except Exception as e:
        logging.error(f"Error uploading file: {e}")
        return func.HttpResponse(f"Error uploading file: {str(e)}", status_code=500)


# Register the HTTP trigger
@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("HTTP trigger function processed a request.")

    action = req.params.get("action")
    if action == "upload":
        return upload_file_to_blob(req)

    return func.HttpResponse("Invalid action. Use 'upload' for uploading files.", status_code=400)


# Blob Trigger to process uploaded files
@app.blob_trigger(arg_name="myblob", path="filestore/uploads/{name}", connection="AzureWebJobsStorage")
def blob_trigger(myblob: func.InputStream, name: str):
    logging.info(f"Blob Trigger - Processing file: {name}, Size: {myblob.length} bytes")
    
    try:
        # Read and process CSV
        file_content = myblob.read()
        df = pd.read_csv(BytesIO(file_content))

        # Validate and analyze data
        required_columns = {"Date", "Open", "High", "Low", "Close", "Volume"}
        if not required_columns.issubset(df.columns):
            logging.error("Uploaded CSV is missing required columns.")
            return

        insights = analyze_stock_data(df)

        # Save results to 'results/' folder in the same container
        results_blob_name = f"results/analysis_{name}"
        results_blob_client = blob_service_client.get_blob_client(container=UPLOAD_CONTAINER, blob=results_blob_name)
        results_blob_client.upload_blob(pd.DataFrame([insights]).to_csv(index=False), overwrite=True)

        logging.info(f"Analysis results saved to: {results_blob_name}")
    except Exception as e:
        logging.error(f"Error processing blob '{name}': {e}")


# Stock data analysis function
def analyze_stock_data(df: pd.DataFrame) -> dict:
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values(by="Date")

    # Perform analysis
    insights = {
        "highest_price": df["High"].max(),
        "lowest_price": df["Low"].min(),
        "average_price": df["Close"].mean(),
        "total_trading_volume": df["Volume"].sum(),
        "start_date": df["Date"].min().strftime("%Y-%m-%d"),
        "end_date": df["Date"].max().strftime("%Y-%m-%d"),
        "performance_change": f"{((df['Close'].iloc[-1] - df['Close'].iloc[0]) / df['Close'].iloc[0]) * 100:.2f}%",
    }

    return insights
