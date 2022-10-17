# Internal dependencies
from modules.api_calls import ApiCalls
from modules.auxiliar import Config, FileProcessing

# External libraries
import datetime as dt
import logging
import pandas as pd

configuration = Config('config.yaml')

# Logging format and configuration file
log_format = '%(asctime)s - %(message)s'
logging.basicConfig(filename=configuration.log_file, format=log_format, level=logging.INFO, force=True)

files = FileProcessing(local_process=True)
calls = ApiCalls(configuration)

def main():
    dataframe_list: list[pd.Dataframe]  = list()
    dataframe_names: list[str]          = list()

    # Downloading all form information
    bt = dt.datetime.now()
    dataframe_list.extend(calls.get_form_data())
    dataframe_names.extend(['forms', 'form_sections', 'form_questions', 'form_options'])

    logging.info(f'Time elapsed for form data: {dt.datetime.now()-bt}')
    
    start   = configuration.days_start
    end     = configuration.days_end
    run, all_records = calls.load_records(days_start = start, days_end = end, all_records = True)

    if run == False:
        logging.warning('The process completed as no data was found for the time period')
        return
    
    run, eval_records = calls.load_records(days_start = start, days_end = end, all_records = False)

    dataframe_list.extend([all_records, eval_records])
    dataframe_names.extend(['all_records', 'eval_records'])

    '''
    Once all dataframes are loaded, export them to individual parquet files.
    
    for item in range(0, len(dataframe_list)):
        files.export(dataframe_list[item], dataframe_names[item], bt)
    '''
    
    logging.info('The process completed successfully')

    return dataframe_list, dataframe_names

if __name__ == '__main__':
    main()