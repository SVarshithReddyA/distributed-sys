import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient, ContentSettings
from werkzeug.utils import secure_filename
import logging
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO
import os

# Initialize the Azure Function app
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

connection_string = os.getenv("WEBSITE_CONTENTAZUREFILECONNECTIONSTRING")
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
    

@app.blob_trigger(arg_name="myblob", path="filestore/{name}",connection="AzureWebJobsStorage") 
def blobTrigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"f"Name: {myblob.name}"f"Blob Size: {myblob.length} bytes")
    logging.info(f"Blob Trigger - Processing file: {myblob.name}, Size: {myblob.length} bytes")
    
    try:
        # Read the CSV file from the blob
        file_content = myblob.read()
        df = pd.read_csv(BytesIO(file_content))

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()

        # Log the columns to inspect them
        logging.info(f"CSV columns: {df.columns}")

        # Validate CSV format
        required_columns = {"Date", "Close/Last", "Volume", "Open", "High", "Low"}
        if not required_columns.issubset(df.columns):
            logging.error(f"Uploaded CSV missing required columns. Found: {df.columns}")
            return

        # Perform Stock Analysis
        analysis_results, chart_buffer = analyze_stock_data(df)

        # Save results back to a Blob
        # Convert analysis results to a DataFrame
        results_df = pd.DataFrame([analysis_results])
        results_csv = results_df.to_csv(index=False)

        # Upload the CSV to Blob Storage
        results_blob_client = blob_service_client.get_blob_client(container=UPLOAD_CONTAINER, blob=f"results_{myblob.name}")
        results_blob_client.upload_blob(results_csv, overwrite=True)
        # Upload the chart as a blob
        chart_blob_client = blob_service_client.get_blob_client(
            container=UPLOAD_CONTAINER, 
            blob=f"chart_{myblob.name.replace('.csv', '.png')}"
        )

        # Reset the buffer pointer to the beginning
        chart_buffer.seek(0)  
        chart_blob_client.upload_blob(
            chart_buffer, 
            overwrite=True, 
            content_settings=ContentSettings(content_type='image/png')
        )
        logging.info(f"Chart uploaded successfully as chart_{myblob.name}.png")
        logging.info(f"Uploading analysis results to blob: results_{myblob.name}")

    except Exception as e:
        logging.error(f"Error processing file {myblob.name}: {e}")


def analyze_stock_data(df: pd.DataFrame) -> dict:
    try:
        # Replace '-' with '/' in the 'Date' column
        df['Date'] = df['Date'].str.replace('-', '/')

        # Convert 'Date' to datetime
        df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')

        # Log the types to debug
        logging.info(f"'Date' column type: {df['Date'].dtype}")
        logging.info(f"Sample 'Date' values after conversion: {df['Date'].head()}")

        # Ensure no invalid dates remain
        if df['Date'].isna().any():
            logging.error("Invalid dates found after conversion. Please check the input data.")
            raise ValueError("Invalid dates in the 'Date' column.")

        # Clean numeric columns
        numeric_columns = ['Close/Last', 'Open', 'High', 'Low']
        for col in numeric_columns:
            df[col] = df[col].replace({'\$': ''}, regex=True).astype(float)

        df = df.sort_values(by='Date')

        # Calculate insights
        insights = {
            "highest_price": df["High"].max(),
            "lowest_price": df["Low"].min(),
            "average_price": df["Close/Last"].mean(),
            "total_trading_volume": df["Volume"].sum(),
            "start_date": df["Date"].min().strftime('%Y-%m-%d'),
            "end_date": df["Date"].max().strftime('%Y-%m-%d'),
            "performance_change": f"{((df['Close/Last'].iloc[-1] - df['Close/Last'].iloc[0]) / df['Close/Last'].iloc[0]) * 100:.2f}%",
        }

        df['7_day_avg'] = df['Close/Last'].rolling(window=7).mean()
        insights["7_day_average_latest"] = df["7_day_avg"].iloc[-1]

        # Generate a line chart
        chart_buffer = generate_line_chart(df)

        return insights, chart_buffer

    except Exception as e:
        logging.error(f"Error during analysis: {e}")
        raise


def generate_line_chart(df: pd.DataFrame) -> BytesIO:
    # Determine the time span of the data
    if (df['Date'].max() - df['Date'].min()).days > 365:
        # Group by year for multi-year data
        df['Year'] = df['Date'].dt.year
        grouped = df.groupby('Year')['Close/Last'].mean()
        x_labels = grouped.index
        y_values = grouped.values
        title = "Average Closing Price by Year"
    else:
        # Group by month for one-year data
        df['Month'] = df['Date'].dt.to_period('M')
        grouped = df.groupby('Month')['Close/Last'].mean()
        x_labels = grouped.index.astype(str)
        y_values = grouped.values
        title = "Average Closing Price by Month"

    # Create the plot
    plt.figure(figsize=(10, 6))
    plt.plot(x_labels, y_values, marker='o', linestyle='-', color='blue')
    plt.title(title)
    plt.xlabel("Time Period")
    plt.ylabel("Average Closing Price")
    plt.grid()
    plt.xticks(rotation=45, ha='right')

    # Save the plot to a buffer
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    plt.close()
    buffer.seek(0)
    return buffer

@app.route(route="get_from_blob", auth_level=func.AuthLevel.ANONYMOUS)
def get_from_blob(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function for fetching files processed a request.')

    # Get the filePath parameter from the request
    file_path = req.params.get('filePath')
    if not file_path:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            file_path = req_body.get('filePath')

    if not file_path:
        return func.HttpResponse(
            "Please specify a filePath in the query string or request body.",
            status_code=400
        )

    try:
        blob_client = blob_service_client.get_blob_client(container=UPLOAD_CONTAINER, blob=file_path)
        blob_data = blob_client.download_blob().readall()

        # Determine content type for response
        if file_path.endswith(".csv"):
            content_type = "text/csv"
        elif file_path.endswith(".png"):
            content_type = "image/png"
        else:
            content_type = "application/octet-stream"

        # Return the file content as an HTTP response
        return func.HttpResponse(
            body=blob_data,
            status_code=200,
            mimetype=content_type
        )
    except Exception as e:
        logging.error(f"Error fetching blob {file_path}: {e}")
        return func.HttpResponse(f"Error fetching blob {file_path}: {str(e)}", status_code=500)
