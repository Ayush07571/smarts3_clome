import boto3

# S3 client configured for LocalStack
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

def list_files(bucket_name):
    response = s3.list_objects_v2(Bucket=bucket_name)
    if "Contents" in response:
        return [obj["Key"] for obj in response["Contents"]]
    return []

def upload_file(bucket_name, file_name, object_name=None):
    if object_name is None:
        object_name = file_name
    s3.upload_file(file_name, bucket_name, object_name)

def download_file(bucket_name, object_name, file_name):
    s3.download_file(bucket_name, object_name, file_name)

def list_objects_pagewise(bucket, prefix="", page_size=1000):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": page_size}):
        for obj in page.get("Contents", []):
            yield obj  # { Key, LastModified, Size, ETag, StorageClass? }