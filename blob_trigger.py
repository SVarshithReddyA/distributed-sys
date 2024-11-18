import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import azure.functions as func

blob_trigger = func.Blueprint()

# Set up Blob Service Client (use environment variables for security)
connection_string = "DefaultEndpointsProtocol=https;AccountName=distributedsys;AccountKey=jLyjj/LyKtRVjlVfdawM5uaBhQZfBUiec+9xxiPxgWVXzn/yng4Rp6/XZojKSjVWZbejiBXuZ+MS+ASteWFJaA==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Define the container for processed results
RESULTS_CONTAINER = "results"

@blob_trigger.blob_trigger(arg_name="myblob", path="filestore/{name}",
                           connection="AzureWebJobsStorage")
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

