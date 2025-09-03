from s3_manager import list_files, upload_file
import boto3

def main():
    bucket = "test-bucket"

    print("📂 CloudSense Prototype Running...")

    # List files in bucket
    files = list_files(bucket)
    print("Files in bucket:", files)

    # Upload another file (for testing)
    with open("newfile.txt", "w") as f:
        f.write("Another test file")

    upload_file(bucket, "newfile.txt")
    print("Uploaded newfile.txt")

    files = list_files(bucket)
    print("Updated files in bucket:", files)

if __name__ == "__main__":
    main()
