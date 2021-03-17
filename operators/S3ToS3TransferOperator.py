# Forked from https://gist.github.com/rchamarthi/4f1fbb3f79048df655cf3f5f4437db31

import logging

from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class S3ToS3TransferOperator(BaseOperator):
    """
    Copies data from a source S3 to a target s3 location.
    This is useful when source and destination buckets are in different
    accounts and access is provided using two sets of AWS keys instead of
    cross-account access policies.

    :param source_s3_key: The key to be retrieved from S3
    :type source_s3_key: str
    :param source_aws_conn_id: source s3 connection
    :type source_aws_conn_id: str
    :param dest_s3_key: The key to be written from S3
    :type dest_s3_key: str
    :param dest_aws_conn_id: destination s3 connection
    :type dest_aws_conn_id: str
    :param replace: Replace dest S3 key if it already exists
    :type replace: bool

    """

    @apply_defaults
    def __init__(
            self,
            source_s3_key,
            dest_s3_key,
            source_aws_conn_id,
            dest_aws_conn_id,
            replace=False,
            *args, **kwargs):
        super(S3ToS3TransferOperator, self).__init__(*args, **kwargs)
        self.source_s3_key = source_s3_key
        self.source_aws_conn_id = source_aws_conn_id
        self.dest_s3_key = dest_s3_key
        self.dest_aws_conn_id = dest_aws_conn_id
        self.replace = replace

    def execute(self, context):

        source_s3 = S3Hook(self.source_aws_conn_id)
        dest_s3 = S3Hook(self.dest_aws_conn_id)

        logging.info("Downloading source S3 file %s", self.source_s3_key)

        # Check if key exists in source directory:
        if not source_s3.check_for_key(self.source_s3_key):
            raise AirflowException(
                "The source key {0} does not exist".format(self.source_s3_key))
        else:
            # If key exists in source directory, assign it to a handler:
            source_s3_key_object = source_s3.get_key(self.source_s3_key)

            # Load to destination S3 key:
            dest_s3.load_string(
                string_data=source_s3_key_object.get()['Body'].read().decode('utf-8'),
                key=self.dest_s3_key,
                replace=self.replace)

            logging.info("Copy successful.")
