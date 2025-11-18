#!/bin/bash

# Script to download an image, upload it to S3, and generate a presigned URL
# Usage: ./upload_and_presign.sh <local_file> <bucket_name> <expiration_seconds>

# Check if correct number of arguments provided
if [ $# -ne 3 ]; then
    echo "Error: Incorrect number of arguments"
    echo "Usage: $0 <local_file> <bucket_name> <expiration_seconds>"
    echo "Example: $0 uva_image.jpg my-bucket-name 604800"
    exit 1
fi

# Assign positional arguments to variables
LOCAL_FILE=$1
BUCKET_NAME=$2
EXPIRATION=$3

# Download the UVA image using curl
echo "Downloading UVA image..."
curl https://www.virginia.edu/assets/images/about/uva-anthem-poster-1800x1013.jpg > "$LOCAL_FILE"

# Check if download was successful
if [ $? -ne 0 ]; then
    echo "Error: Failed to download image"
    exit 1
fi

echo "Image downloaded successfully as '$LOCAL_FILE'"
echo ""

# Check if local file exists (should exist after download)
if [ ! -f "$LOCAL_FILE" ]; then
    echo "Error: File '$LOCAL_FILE' does not exist"
    exit 1
fi

echo "Uploading '$LOCAL_FILE' to s3://$BUCKET_NAME/$LOCAL_FILE..."

# Upload file to S3
aws s3 cp "$LOCAL_FILE" "s3://$BUCKET_NAME/"

# Check if upload was successful
if [ $? -eq 0 ]; then
    echo "Upload successful!"
    echo ""
    
    # List bucket contents to verify upload
    echo "Bucket contents:"
    aws s3 ls "s3://$BUCKET_NAME/"
    echo ""
    
    echo "Generating presigned URL with expiration of $EXPIRATION seconds..."
    
    # Generate presigned URL
    PRESIGNED_URL=$(aws s3 presign "s3://$BUCKET_NAME/$LOCAL_FILE" --expires-in "$EXPIRATION")
    
    # Check if presign was successful
    if [ $? -eq 0 ]; then
        echo ""
        echo "Presigned URL generated successfully:"
        echo "$PRESIGNED_URL"
        echo ""
        echo "This URL will expire in $EXPIRATION seconds ($(($EXPIRATION / 60)) minutes)"
        echo ""
        echo "Test this URL in your browser. After $EXPIRATION seconds, refresh and it should show 'Access Denied'"
    else
        echo "Error: Failed to generate presigned URL"
        exit 1
    fi
else
    echo "Error: Upload failed"
    exit 1
fi

