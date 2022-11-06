import logging
import pandas as pd
from typing import Tuple
from cryptography.fernet import Fernet

class Config():
    def __init__(self, filename: str):
        # External libraries:
        # from cryptography.fernet import Fernet
        from os import getenv
        from ruamel.yaml import round_trip_load, round_trip_dump

        self.filename = filename
        
        try:
            with open(filename, 'r') as f:
                self.configuration = round_trip_load(f)

            logging.info('Configuration read successfully')

            # API Connection Info
            self.api_url        = self.configuration['api']['url']
            self.api_user       = self.configuration['api']['user']

            ''' Use a hard-coded plain-text password if you don't have an encryption method,
            else, refer to the token method below '''
            # self.api_password   = ''

            token               = self.configuration['general']['token']

            cphr = Fernet(getenv('py_key'))
            tkn_fl = open(token, 'rb')
            tkn = tkn_fl.read()
            self.api_password = cphr.decrypt(tkn).decode('utf-8')

            # Data for the run
            self.start_date     = self.configuration['general']['start_date']
            self.log_file       = self.configuration['general']['log_file']
            
            # S3 information
            self.bucket_name    = self.configuration['general']['bucket']
            self.s3_path        = self.configuration['general']['path']

        except Exception as e:
            logging.exception(e)

    def update(self, configuration: str):
        from ruamel.yaml import round_trip_dump

        with open(self.filename, "w") as f:
            round_trip_dump(configuration, f)

class FileProcessing():
    # External libraries:
    import datetime as dt
    import pandas as pd

    def __init__(self, chunk_size: int = 100000, local_process: bool = False) -> None:
        self.chunk_size = chunk_size
        self.local_process = local_process
    
    def group_dfs(self, calls) -> Tuple[pd.DataFrame, pd.DataFrame]:
        dataframe_list: list[pd.DataFrame]  = list()
        dataframe_names: list[str]          = list()

        dataframe_list.extend([calls.df_all_records, calls.df_eval_records])
        dataframe_names.extend(['all_records', 'eval_records'])

        dataframe_list.extend([calls.df_forms, calls.df_form_sections, calls.df_form_questions, calls.df_form_options])
        dataframe_names.extend(['forms', 'form_sections', 'form_questions', 'form_options'])

        dataframe_list.extend([calls.df_eval_details, calls.df_eval_sections, calls.df_eval_questions, calls.df_eval_comments])
        dataframe_names.extend(['eval_details', 'eval_sections', 'eval_questions', 'eval_comments'])
        
        dataframe_list.extend([calls.df_agents, calls.df_schedules])
        dataframe_names.extend(['agents', 'schedules'])
        return dataframe_list, dataframe_names

    def split_chunks(self, df):
        from math import ceil

        """
        Divide bigger datasets into chunks.
        This function will mainly be used when loading historical data for all records
        """

        chunks = list()
        num_chunks = ceil(len(df) / self.chunk_size)

        for i in range(num_chunks):
            start_index = i     * self.chunk_size
            end_index   = (i+1) * self.chunk_size
            chunks.append(df[start_index:end_index])

        return chunks

    def export(self, data: pd.DataFrame, table: str, bt: dt.datetime):

        from pathlib import Path
        from os import path, makedirs

        """
        Upload the chunked datasets to S3, or generate them locally.
        """

        yy = bt.year

        # Formatting to two digits
        mm = str(bt.month).zfill(2)
        dd = str(bt.day).zfill(2)

        if(self.local_process == False):
            bucket  = self.cfg.bucket_name
            s3_path = f'{self.cfg.project}/{table.lower()}'
            
            base_path = f's3://{bucket}/{s3_path}/{yy}/{mm}/{dd}/'
            
        else:
            base_path = f'./output/{table.lower()}/{yy}/{mm}/{dd}'
            if not path.exists(base_path):
                makedirs(base_path)

        chunks = self.split_chunks(data)
        logging.info(f"\t{len(chunks)} chunks will be generated for table - {table.replace('/', '.')}")
        logging.info(f'\tExporting to {base_path}')

        for iter, chunk in enumerate(chunks):
            # Formatting to three digits
            file_name = f'Chunk_{str(iter).zfill(3)}'
            self.pd.DataFrame(chunk).to_parquet(
                path=f'{base_path}/{file_name}.parquet',
                compression='gzip', index=False)