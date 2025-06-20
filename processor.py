import boto3
import pandas as pd
import time
import json
from io import BytesIO

# Configuration
S3_INPUT_BUCKET = "newfiles7910"
S3_OUTPUT_BUCKET = "output568"
SQS_URL = "https://sqs.us-east-1.amazonaws.com/211125718182/fileprocessor"
S3_REGION = "us-east-1"

# AWS clients
s3 = boto3.client("s3", region_name=S3_REGION)
sqs = boto3.client("sqs", region_name=S3_REGION)

def process_file(s3_key):
    try:
        print(f"Downloading file: {s3_key}")
        
        # Get the object from S3
        obj = s3.get_object(Bucket=S3_INPUT_BUCKET, Key=s3_key)
        
        # Wrap in BytesIO to make it seekable
        body = obj["Body"].read()
        file_buffer = BytesIO(body)
        
        # Now read Excel from seekable buffer
        df = pd.read_excel(file_buffer, engine="openpyxl")

        # Process (top 10 rows)
        processed_df = df.head(10)

        # Write to output buffer
        output_buffer = BytesIO()
        with pd.ExcelWriter(output_buffer, engine="xlsxwriter") as writer:
            processed_df.to_excel(writer, index=False, sheet_name="Sheet1")
        output_buffer.seek(0)

        # Upload to output bucket
        output_key = f"processed/{s3_key.split('/')[-1]}"
        s3.upload_fileobj(output_buffer, S3_OUTPUT_BUCKET, output_key)
        print(f"✅ Uploaded processed file to: s3://{S3_OUTPUT_BUCKET}/{output_key}")
    
    except Exception as e:
        print(f"❌ Failed to process {s3_key}: {e}")

def poll_sqs():
    print("Polling SQS for messages...")
    while True:
        response = sqs.receive_message(
            QueueUrl=SQS_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        messages = response.get("Messages", [])
        if not messages:
            continue

        for msg in messages:
            try:
                body = json.loads(msg["Body"])
                s3_key = body["s3_key"]

                process_file(s3_key)

                # Delete message from queue after successful processing
                sqs.delete_message(
                    QueueUrl=SQS_URL,
                    ReceiptHandle=msg["ReceiptHandle"]
                )
                print(f"✅ Message processed and deleted from queue.")
            except Exception as e:
                print(f"❌ Error handling message: {e}")

if __name__ == "__main__":
    poll_sqs()

