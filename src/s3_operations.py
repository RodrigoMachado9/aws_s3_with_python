import boto3
import json
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


if __name__ == '__main__':
    # create_bucket(BUCKET_NAME)
    # print(create_bucket_policy())
    # print(list_buckets())
    # print(get_bucket_policy())
    # print(get_bucket_encryption())
    print(update_bucket_policy())

