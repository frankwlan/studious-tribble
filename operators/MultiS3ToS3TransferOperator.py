import logging

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class MultiS3ToS3TransferOperator(BaseOperator):
    """
    Copies data from a source AWS S3 directory to a target AWS S3 directory.
    Intended for use when the source and target directories are on different
    AWS accounts and access is provided using two sets of AWS keys instead of
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
        *args, **kwargs
    ):
        super(MultiS3ToS3TransferOperator, self).__init__(*args, **kwargs)
        self.source_aws_conn_id = source_aws_conn_id
        self.source_s3_bucket = source_s3_bucket
        self.source_s3_dir = source_s3_dir
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_s3_bucket = dest_s3_bucket
        self.dest_s3_dir = dest_s3_dir
        self.replace = replace

    def execute(self, context):

        source_s3_hook = S3Hook(aws_conn_id=self.source_aws_conn_id)
        dest_s3_hook = S3Hook(aws_conn_id=self.dest_aws_conn_id)

        # Confirm directory exists in source bucket:
        if not source_s3_hook.check_for_prefix(
            bucket_name = self.source_s3_bucket,
            prefix = self.source_s3_dir,
            delimiter = '/'
        ):
            raise AirflowFailException(
                f"The source {self.source_s3_dir} does not exist."
            )
        # Check if destination directory already exists:
        elif dest_s3_hook.check_for_prefix(
            bucket_name = self.dest_s3_bucket,
            prefix = self.dest_s3_dir,
            delimiter = '/'
        ) and not self.replace:
            raise AirflowSkipException(
                f"The dest {self.dest_s3_dir} already exists."
            )
        # Confirmed directory exists in source and destination directory does
        # not already exist, or should be replaced, so upload directory:
        else:
            logging.info(
                "Downloading source S3 directory %s to %s",
                self.source_s3_dir, self.dest_s3_dir
            )

            source_s3_dir_keys = source_s3_hook.list_keys(
                bucket_name = self.source_s3_bucket,
                prefix = self.source_s3_dir,
                delimiter = '/'
            )

            for source_s3_dir_key in source_s3_dir_keys:

                s3_key_obj = source_s3_hook.get_key(
                    key=source_s3_dir_key,
                    bucket_name=self.source_s3_bucket
                )

                # list_keys returns the key with the directory included.
                # Only file name needed:
                source_s3_key = source_s3_dir_key.replace(self.source_s3_dir, '')

                dest_s3_hook.load_string(
                    string_data=s3_key_obj.get()['Body'].read().decode('utf-8'),
                    key=self.dest_s3_dir + source_s3_key,
                    bucket_name=self.dest_s3_bucket,
                    replace=self.replace
                )

                logging.info(
                    f"Copied {self.dest_s3_dir}{source_s3_key} successfully."
                )

            logging.info(
                f"Success: Copy {self.source_s3_dir} to {self.dest_s3_dir}"
            )
