from tempfile import NamedTemporaryFile
from os import remove
from urllib.parse import urlparse

from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults

class MultiSFTPToS3Operator(BaseOperator):
    """
    This operator enables the transferring of files from a SFTP server to S3.

    :param sftp_conn_id: The sftp connection id.
    :type sftp_conn_id: str
    :param sftp_path: The sftp remote path.
    :type sftp_path: str
    :param s3_conn_id: The s3 connection id.
    :type s3_conn_id: str
    :param s3_bucket: The targeted s3 bucket.
    :type s3_bucket: str
    :param s3_key: The targeted s3 key.
    :type s3_key: str

    """

    @apply_defaults
    def __init__(
            self,
            sftp_conn_id,
            sftp_path,
            s3_conn_id,
            s3_bucket,
            s3_key,
            *args, **kwargs):
        super(MultiSFTPToS3Operator, self).__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    @staticmethod
    def get_s3_key(s3_key):
        # Parses the correct format for S3 keys:

        parsed_s3_key = urlparse(s3_key)
        return parsed_s3_key.path.lstrip('/')

    def execute(self, context):

        self.s3_key = self.get_s3_key(self.s3_key)

        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        sftp_client = ssh_hook.get_conn().open_sftp()

        files = sftp_client.listdir(self.sftp_path)

        self._log.info(files)
        
        for file in files:
            with NamedTemporaryFile("w") as f:

                print('Downloading ' + self.sftp_path + file + ' as ' + f.name)
                sftp_client.get(
                    self.sftp_path + file,
                    f.name)
                print('Finished downloading ' + self.sftp_path + file + ' as '
                      + f.name)

                print('Uploading ' + f.name + ' to S3 ' + self.s3_bucket + ' '
                      + self.s3_key)
                s3_hook.load_file(
                    filename=f.name,
                    key=self.s3_key,
                    bucket_name=self.s3_bucket,
                    replace=True)
                print('Finished uploading ' + f.name + ' to S3 '
                      + self.s3_bucket + ' ' + self.s3_key)

                print('Deleting: ' + f.name)
                os.remove(f.name)
        
        print('Closing SFTP client.')
        sftp_client.close()
        print('Closed SFTP client successfully.')
