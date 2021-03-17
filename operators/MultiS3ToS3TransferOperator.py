import logging

from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class MultiS3ToS3TransferOperator(BaseOperator):
    """
    Copies data from a source AWS S3 directory to a target AWS S3 directory.
    This is useful when source and destination buckets are in different
    accounts and access is provided using two sets of AWS keys instead of
    cross-account access policies.

    :param source_aws_conn_id: source s3 connection
    :type source_aws_conn_id: str
    :param source_s3_bucket: source s3 bucket
    :type source_s3_bucket: str
    :param source_s3_dir: The directory to be retrieved from source S3
    :type source_s3_dir: str
    :param dest_aws_conn_id: destination s3 connection
    :type dest_aws_conn_id: str
    :param dest_s3_bucket: source s3 bucket
    :type dest_s3_bucket: str
    :param dest_s3_dir: The directory to be written to from source S3
    :type dest_s3_dir: str
    :param replace: Replace dest S3 key if it already exists. Default False
    :type replace: bool

    """

    @apply_defaults
    def __init__(
            self,
            source_aws_conn_id,
            source_s3_bucket,
            source_s3_dir,
            dest_aws_conn_id,
            dest_s3_bucket,
            dest_s3_dir,
            replace=False,
            *args, **kwargs):
        super(MultiS3ToS3TransferOperator, self).__init__(*args, **kwargs)
        self.source_aws_conn_id = source_aws_conn_id
        self.source_s3_bucket = source_s3_bucket
        self.source_s3_dir = source_s3_dir
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_s3_bucket = dest_s3_bucket
        self.dest_s3_dir = dest_s3_dir
        self.replace = replace

    def execute(self, context):

        source_s3 = S3Hook(self.source_aws_conn_id)
        dest_s3 = S3Hook(self.dest_aws_conn_id)

        logging.info("Downloading source S3 directory %s", self.source_s3_dir)

        # Confirm directory exists in source bucket:
        if not source_s3.check_for_prefix(
            bucket_name = self.source_s3_bucket,
            prefix = self.source_s3_dir,
            delimiter = '/'):
            raise AirflowException(
                "The source {0} does not exist".format(self.source_s3_dir))
        else:
            source_s3_dir_keys = source_s3.list_keys(
                bucket_name = self.source_s3_bucket,
                prefix = self.source_s3_dir,
                delimiter = '/')

            for source_s3_key in source_s3_dir_keys:

                source_s3_key_obj = source_s3.get_key(source_s3_key)
                
                dest_s3.load_string(
                    string_data=source_s3_key_obj.get()['Body'].read().decode('utf-8'),
                    key=self.dest_s3_dir + source_s3_key,
                    replace=self.replace)
                logging.info(
                    "Success: Copy " + source_s3_key + " to "
                    + self.dest_s3_dir + source_s3_key)

            logging.info(
                "Success: Copy " + self.source_s3_dir + " to " + self.dest_s3_dir)
