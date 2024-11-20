import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient, ContentSettings
from werkzeug.utils import secure_filename
import logging
import pandas as pd
import matplotlib.pyplot as plt
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
    """
    Blob trigger function to analyze uploaded stock data and generate insights.
    """
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
        required_columns = {"Date", "Close", "Volume", "Open", "High", "Low"}
        if not required_columns.issubset(df.columns):
            logging.error(f"Uploaded CSV missing required columns. Found: {df.columns}")
            return

        # Perform Stock Analysis
        analysis_results, chart_buffer = analyze_stock_data(df)

        # Save results back to a Blob
        # Convert analysis results to a DataFrame
        results_df = pd.DataFrame([analysis_results])  # Wrap the dictionary in a list
        results_csv = results_df.to_csv(index=False)

        # Upload the CSV to Blob Storage
        
        results_blob_client = blob_service_client.get_blob_client(container=UPLOAD_CONTAINER, blob=f"results_{myblob.name}")
        results_blob_client.upload_blob(results_csv, overwrite=True)
        # Upload the chart as a blob
        chart_blob_client = blob_service_client.get_blob_client(
            container=UPLOAD_CONTAINER, 
            blob=f"chart_{myblob.name}.png"
        )
        chart_buffer.seek(0)  # Reset the buffer pointer to the beginning
        chart_blob_client.upload_blob(
            chart_buffer, 
            overwrite=True, 
            content_settings=ContentSettings(content_type='image/png')
        )
        logging.info(f"Chart uploaded successfully as chart_{myblob.name}.png")

        # results_blob_client.upload_blob(chart_buffer, overwrite=True, content_settings=ContentSettings(content_type='image/png'))
        logging.info(f"Uploading analysis results to blob: results_{myblob.name}")
        # logging.info(f"Uploading chart to blob: chart_{myblob.name}.png")

    except Exception as e:
        logging.error(f"Error processing file {myblob.name}: {e}")


def analyze_stock_data(df: pd.DataFrame) -> dict:
    """
    Perform analysis on stock data and return insights and a line chart.
    """
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
        numeric_columns = ['Close', 'Open', 'High', 'Low']
        for col in numeric_columns:
            df[col] = df[col].replace({'\$': ''}, regex=True).astype(float)

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

        # Generate a line chart
        chart_buffer = generate_line_chart(df)

        return insights, chart_buffer

    except Exception as e:
        logging.error(f"Error during analysis: {e}")
        raise


def generate_line_chart(df: pd.DataFrame) -> BytesIO:
    """
    Generate a line chart for stock data and save it to a buffer.
    """
    # Determine the time span of the data
    if (df['Date'].max() - df['Date'].min()).days > 365:
        # Group by year for multi-year data
        df['Year'] = df['Date'].dt.year
        grouped = df.groupby('Year')['Close'].mean()
        x_labels = grouped.index
        y_values = grouped.values
        title = "Average Closing Price by Year"
    else:
        # Group by month for one-year data
        df['Month'] = df['Date'].dt.to_period('M')
        grouped = df.groupby('Month')['Close'].mean()
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
