from boto3.s3.transfer import TransferConfig
import threading
import boto3
import json
import os
import sys

BUCKET_NAME = 'rmachado-development'


def s3_client():
    s3 = boto3.client('s3')
    """ :type : pyboto3.s3"""
    return s3

def create_bucket(bucket_name):
    return s3_client().create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            "LocationConstraint": "us-west-2"
        }
    )

def create_bucket_policy():
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AddPerm",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:*"],
                "Resource": ["arn:aws:s3:::rmachado-development"]
            }
        ]
    }
    policy_string = json.dumps(bucket_policy)
    return s3_client().put_bucket_policy(
        Bucket=BUCKET_NAME,
        Policy=policy_string
    )


def list_buckets():
    return s3_client().list_buckets()


def get_bucket_policy():
    return s3_client().get_bucket_policy(
        Bucket=BUCKET_NAME
    )


def get_bucket_encryption():
    # s3_client().get_paginator()
    return s3_client().get_bucket_encryption(Bucket=BUCKET_NAME)


def get_bucket_cors():
    return s3_client().get_bucket_cors(Bucket=BUCKET_NAME)


def update_bucket_policy():
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AddPerm",
                "Effect": "Allow",
                "Principal": "*",
                "Action": [
                    "s3:DeleteObject",
                    "s3:GetObject",
                    "s3:PutObject"
                ],
                "Resource": "arn:aws:s3:::rmachado-development/*"
            }
        ]
    }
    policy_string = json.dumps(bucket_policy)
    return s3_client().put_bucket_policy(
        Bucket=BUCKET_NAME,
        Policy=policy_string
    )

def server_side_encrypt_bucket():
    return s3_client().put_bucket_encryption(
        Bucket=BUCKET_NAME,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"
                    }
                }
            ]
        }
    )


def delete_bucket():
    return s3_client().delete_bucket(Bucket=BUCKET_NAME)

def upload_small_file():
    file_path = os.path.dirname(__file__) + '/readme.txt'
    return s3_client().upload_file(file_path, BUCKET_NAME, '2020/readme.txt')


def upload_large_file():
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    file_path = os.path.dirname(__file__) + '/codigo_limpo.pdf'
    key_path = 'multipart_files/codigo_limpo.pdf'

    s3_resource().meta.client.upload_file(file_path, BUCKET_NAME, key_path, ExtraArgs={
        'ACL': 'public-read',
        'ContentType': 'text/pdf'},
                                          Config=config,
                                          Callback=ProgressPercentage(file_path)
                                          )
class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (self._filename, self._seen_so_far, self._size, percentage))
            sys.stdout.flush()




def s3_resource():
    # s3 = boto3.resource('s3')
    return boto3.resource('s3')


def read_object_from_bucket():
    object_key = '2020/readme.txt'
    return s3_client().get_object(Bucket=BUCKET_NAME, Key=object_key)




if __name__ == '__main__':
    # print(create_bucket(BUCKET_NAME))
    # print(create_bucket_policy())
    # print(list_buckets())
    # print(get_bucket_policy())
    # print(get_bucket_encryption())
    # print(update_bucket_policy())
    # print(server_side_encrypt_bucket())
    # print(delete_bucket())
    # print('created is success' if upload_small_file() is None else None)
    # print(upload_large_file())
    print(read_object_from_bucket())