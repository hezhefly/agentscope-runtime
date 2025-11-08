# -*- coding: utf-8 -*-
import os
import hashlib
from minio import Minio
from minio.error import S3Error
from .data_storage import DataStorage


def calculate_md5(file_path):
    """Calculate the MD5 checksum of a file."""
    with open(file_path, "rb") as f:
        md5 = hashlib.md5()
        while chunk := f.read(8192):
            md5.update(chunk)
    return md5.hexdigest()


class MinioStorage(DataStorage):
    def __init__(
        self,
        endpoint,
        access_key,
        secret_key,
        bucket_name,
        secure=True,
    ):
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket_name = bucket_name
        
        # Ensure bucket exists
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)

    def download_folder(self, source_path, destination_path):
        """Download a folder from MinIO to the local filesystem."""
        if not os.path.exists(destination_path):
            os.makedirs(destination_path)

        # Remove any leading slashes for compatibility
        source_path = source_path.lstrip('/')
        # Ensure source path ends with a slash for proper prefix matching
        if not source_path.endswith('/'):
            source_path += '/'

        # List all objects with the given prefix
        objects = self.client.list_objects(
            self.bucket_name,
            prefix=source_path,
            recursive=True
        )

        for obj in objects:
            # Calculate relative path
            relative_path = os.path.relpath(obj.object_name, source_path)
            local_path = os.path.join(destination_path, relative_path)

            # Create directory structure if needed
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            # Download the file
            self.client.fget_object(
                self.bucket_name,
                obj.object_name,
                local_path
            )

    def upload_folder(self, source_path, destination_path):
        """Upload a local folder to MinIO."""
        # Remove any leading slashes for compatibility
        destination_path = destination_path.lstrip('/')
        # Ensure destination path ends with a slash for proper path joining
        if not destination_path.endswith('/'):
            destination_path += '/'

        for root, dirs, files in os.walk(source_path):
            # Upload files
            for file in files:
                local_file_path = os.path.join(root, file)
                # Calculate relative path to maintain directory structure
                relative_path = os.path.relpath(local_file_path, source_path)
                # Convert to MinIO path format (use forward slashes)
                minio_file_path = destination_path + relative_path.replace('\\', '/')

                # Calculate MD5 for comparison
                local_md5 = calculate_md5(local_file_path)

                # Check if file exists in MinIO
                try:
                    stat = self.client.stat_object(
                        self.bucket_name,
                        minio_file_path
                    )
                    # Get ETag (MD5) from MinIO
                    minio_md5 = stat.etag.strip('"')
                    
                    # Skip upload if MD5 matches
                    if local_md5 == minio_md5:
                        continue
                except S3Error as e:
                    # File doesn't exist, so upload it
                    if e.code == 'NoSuchKey':
                        pass
                    else:
                        raise

                # Upload the file
                self.client.fput_object(
                    self.bucket_name,
                    minio_file_path,
                    local_file_path
                )

    def path_join(self, *args):
        """Join path components for MinIO."""
        # MinIO uses forward slashes for paths
        return '/'.join(args)