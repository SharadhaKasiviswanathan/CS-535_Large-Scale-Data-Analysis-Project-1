import boto3

def upload_to_s3(file_path, bucket_name, s3_path):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, s3_path)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_path}")
    except Exception as e:
        print(f"Failed to upload {file_path} to S3: {e}")

# Define the file path, bucket name, and S3 path
file_path = r"C:\Users\Sharadha Kasi\Downloads\lsdaprj1.zip"  # Local path to the zip file
bucket_name = 'sharadhakasi'  # Replace with your S3 bucket name
s3_path = 'code/lsdaprj1.zip'  # Path in the bucket where the file will be uploaded

# Upload the file
upload_to_s3(file_path, bucket_name, s3_path)