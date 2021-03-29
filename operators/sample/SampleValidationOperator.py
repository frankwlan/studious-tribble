import pandas as pd
import tempfile

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator, S3CopyObjectOperator

class RangeCheckOperator(BaseOperator):

    def __init__(
        self,
        # Connection parameters:
        s3_connection_id=None,
        # Source parameters, with example:
        source_bucket=None, # roi-data.pepsi-ecom.co
        source_folder=None, # dsa_automation
        actual_values_file_loc=None, # actual/H_2020_Historic_Contribs.csv
        expected_values_file_loc=None, # expected/Expected_Contribution_By_Trademark.csv
        # Output parameters:
        destination_bucket=None, # roi-data-pipeline
        destination_folder=None, # dsa_automation
        destination_file_loc=None, # output/H_2020_Historic_Ranges.csv
        actual_values_file_finish=None,
        expected_values_file_finish=None,
        *args, **kwargs
    ):

        '''
        Initializes the RangeCheckOperator.

        Connection parameters:
        :param s3_connection_id = 'pepsi_dp_s3'

        Source parameters:
        :param source_bucket: Source S3 bucket.
        :param source_folder: Source folder within the S3 bucket.
        :param actual_values_file: Actual values file S3 path within source_folder.
        :param expected_values_file: Expected values file S3 path within source_folder.

        Output parameters:
        :param destination_bucket: Destination S3 bucket.
        :param destination_folder:  Destination folder within the S3 bucket.
        :param destination_file: Output values file S3 path within destination_folder.
        :param args:
        :param kwargs:
        '''

        # Connection parameters:
        self.s3_connection_id = s3_connection_id
        # Source parameters:
        self.source_bucket = source_bucket
        self.source_folder = source_folder
        self.actual_values_file_loc = actual_values_file_loc
        self.expected_values_file_loc = expected_values_file_loc
        # Output parameters:
        self.destination_bucket = destination_bucket
        self.destination_folder = destination_folder
        self.destination_file_loc = destination_file_loc
        self.actual_values_file_finish = actual_values_file_finish
        self.expected_values_file_finish = expected_values_file_finish

        super(RangeCheckOperator, self).__init__(*args, **kwargs)


    def execute(self, context):
        # Columns from the actual values file:
        ACTUAL_VALUE_COLUMNS = (
            'PRODUCT',
            'CHANNEL',
            'PERIOD',
            'base_actual',
            'trade_actual',
            'media_actual'
        )

        # Columns from the expected values file:
        EXPECTED_VALUE_COLUMNS = (
            'trademark',
            'base_expected',
            'trade_expected',
            'media_expected'
        )

        # Initiate connection:
        s3_conn = S3Hook(self.s3_connection_id)
        source_bucket_conn = s3_conn.get_bucket(self.source_bucket) # Connect to roi-data.pepsi-ecom.co
        destination_bucket_conn = s3_conn.get_bucket(self.destination_bucket) # Connect to roi-data-pipeline

        # Read files from S3:
        # Actual
        with tempfile.TemporaryFile() as temp_actual_values_file:
            source_bucket_conn.download_fileobj(
                self.source_folder + '/' + self.actual_values_file_loc,
                temp_actual_values_file
            )
            temp_actual_values_file.seek(0)
            # Read into DataFrame:
            actual_df = pd.read_csv(
                temp_actual_values_file, header=0, names=ACTUAL_VALUE_COLUMNS
            )

        # Expected
        with tempfile.TemporaryFile() as temp_expected_values_file:
            source_bucket_conn.download_fileobj(
                self.source_folder + '/' + self.expected_values_file_loc,
                temp_expected_values_file
            )
            temp_expected_values_file.seek(0)
            # Read into DataFrame:
            expected_df = pd.read_csv(
                temp_expected_values_file, header=0, names=EXPECTED_VALUE_COLUMNS
            )

        # Calculation logic here:
        # Join actual and expected values into one DataFrame:
        merged = pd.merge(
            actual_df,
            expected_df,
            left_on='PRODUCT',
            right_on='trademark',
            how='left'
        )

        merged['base_difference'] = merged['base_actual'] - merged['base_expected']
        merged['trade_difference'] = merged['trade_actual'] - merged['trade_expected']
        merged['media_difference'] = merged['media_actual'] - merged['media_expected']

        # Reserve space to input range tolerance parameters here. At the time of writing it is 20%

        # Base - In Range
        merged.loc[
            merged['base_actual'] / merged['base_expected'] >= 0.8,
            'base_in_range'
        ] = True
        merged.loc[
            merged['base_actual'] / merged['base_expected'] <= 1.2,
            'base_in_range'
        ] = True

        # Base - Out of Range
        merged.loc[
            merged['base_actual'] / merged['base_expected'] < 0.8,
            'base_in_range'
        ] = False
        merged.loc[
            merged['base_actual'] / merged['base_expected'] > 1.2,
            'base_in_range'
        ] = False

        # Trade - In Range
        merged.loc[
            merged['trade_actual'] / merged['trade_expected'] >= 0.8,
            'trade_in_range'
        ] = True
        merged.loc[
            merged['trade_actual'] / merged['trade_expected'] <= 1.2,
            'trade_in_range'
        ] = True

        # Trade - Out of Range
        merged.loc[
            merged['trade_actual'] / merged['trade_expected'] < 0.8,
            'trade_in_range'
        ] = False
        merged.loc[
            merged['trade_actual'] / merged['trade_expected'] > 1.2,
            'trade_in_range'
        ] = False

        # Media - In Range
        merged.loc[
            merged['media_actual'] / merged['media_expected'] >= 0.8,
            'media_in_range'
        ] = True
        merged.loc[
            merged['media_actual'] / merged['media_expected'] <= 1.2,
            'media_in_range'
        ] = True

        # Media - Out of Range
        merged.loc[
            merged['media_actual'] / merged['media_expected'] < 0.8,
            'media_in_range'
        ] = False
        merged.loc[
            merged['media_actual'] / merged['media_expected'] > 1.2,
            'media_in_range'
        ] = False

        # Upload output to S3:
        f = tempfile.NamedTemporaryFile()
        merged.to_csv(f.name)
        destination_bucket_conn.upload_file(
            f.name, self.destination_folder + '/' + self.destination_file_loc
        )

        # Move from 'need processing' to 'procesed':
        s3_conn.copy_object(
            source_bucket_key=self.source_folder + '/' + self.actual_values_file_loc,
            dest_bucket_key=self.destination_folder + '/' + self.actual_values_file_finish,
            source_bucket_name=source_bucket,
            dest_bucket_name=destination_bucket
        )

        s3_conn.copy_object(
            source_bucket_key=self.source_folder + '/' + self.expected_values_file_loc,
            dest_bucket_key=self.destination_folder + '/' + self.expected_values_file_finish,
            source_bucket_name=source_bucket,
            dest_bucket_name=destination_bucket
        )

        # Remove from source directory:
        s3_conn.delete_objects(
            bucket=source_bucket,
            keys=[
                self.source_folder + '/' + self.actual_values_file_loc,
                self.source_folder + '/' + self.expected_values_file_loc
            ]
        )
