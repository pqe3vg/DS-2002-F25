#!/usr/bin/env python3

import boto3
import requests
import os
import sys

def download_file(url, file_path):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"File downloaded to {file_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading: {e}")

# Check arguments
if len(sys.argv) != 3:
    print("Usage: python3 script.py <bucket_name> <expiration_seconds>")
    sys.exit(1)

bucket_name = sys.argv[1]
expires_in = int(sys.argv[2])

# Download image
image_url = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTYnoX2alO-tJUygTdeBD5r2fJpdWmBQvi1ow&s"
file = "downloaded_image.jpg"
path = os.path.join(os.getcwd(), file)

download_file(image_url, path)

# Upload to S3
s3 = boto3.client('s3')
s3.upload_file(path, bucket_name, file)
print(f"Uploaded to s3://{bucket_name}/{file}")

# Generate presigned URL
response = s3.generate_presigned_url(
    'get_object',
    Params={'Bucket': bucket_name, 'Key': file},
    ExpiresIn=expires_in
)

print(f"\nPresigned URL:\n{response}")
print(f"\nExpires in {expires_in} seconds")

