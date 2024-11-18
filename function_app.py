import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from werkzeug.utils import secure_filename
import logging
import pandas as pd
from io import BytesIO

# Initialize the Azure Function app
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
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
    try:
        # Access file content directly from the request
        file = req.files.get("file")  # Get the file from multipart form-data
        if not file:
            return func.HttpResponse("No file provided", status_code=400)

        # Derive filename from the uploaded file or set a default if unavailable
        filename = secure_filename(file.filename) if hasattr(file, 'filename') else "uploaded_file"

        # Upload to blob
        blob_client = blob_service_client.get_blob_client(container=UPLOAD_CONTAINER, blob=filename)
        blob_client.upload_blob(file.read(), overwrite=True)  # Read file content and upload

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
    

@app.blob_trigger(arg_name="myblob", path="filestore/{name}",
                               connection="AzureWebJobsStorage") 
def blobTrigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
def process_stock_data(myblob: func.InputStream, name: str):
    """
    Blob trigger function to analyze uploaded stock data and generate insights.
    """
    logging.info(f"Blob Trigger - Processing file: {name}, Size: {myblob.length} bytes")
    
    try:
        # Read the CSV file from the blob
        file_content = myblob.read()
        df = pd.read_csv(BytesIO(file_content))

        # Validate CSV format
        required_columns = {"Date", "Open", "High", "Low", "Close", "Volume"}
        if not required_columns.issubset(df.columns):
            logging.error("Uploaded CSV missing required columns.")
            return
        
        # Perform Stock Analysis
        analysis_results = analyze_stock_data(df)

        # Save results back to a Blob (or send as a response if using HTTP)
        results_blob_client = blob_service_client.get_blob_client(container=RESULTS_CONTAINER, blob=f"results_{name}")
        results_blob_client.upload_blob(pd.DataFrame(analysis_results).to_csv(index=False), overwrite=True)

        logging.info(f"Analysis results saved to Blob Storage: results_{name}")

    except Exception as e:
        logging.error(f"Error processing file {name}: {e}")

def analyze_stock_data(df: pd.DataFrame) -> dict:
    """
    Perform analysis on stock data and return insights.
    """
    # Ensure 'Date' is datetime
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values(by='Date')

    # Calculate insights
    insights = {
        "highest_price": df["High"].max(),
        "lowest_price": df["Low"].min(),
        "average_price": df["Close"].mean(),
        "total_trading_volume": df["Volume"].sum(),
        "start_date": df["Date"].min().strftime('%Y-%m-%d'),
        "end_date": df["Date"].max().strftime('%Y-%m-%d'),
        "performance_change": f"{((df['Close'].iloc[-1] - df['Close'].iloc[0]) / df['Close'].iloc[0]) * 100:.2f}%",
    }

    # Example: Add rolling average or volatility if needed
    df['7_day_avg'] = df['Close'].rolling(window=7).mean()
    insights["7_day_average_latest"] = df["7_day_avg"].iloc[-1]

    return insights
