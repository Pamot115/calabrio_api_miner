# Internal dependencies
from modules.api_calls import ApiCalls
from modules.auxiliar import Config, FileProcessing

# External libraries
from dateutil.relativedelta import relativedelta
import datetime as dt
import logging
import pandas as pd

cfg = Config('config.yaml')

# Logging format and configuration file
log_format = '%(asctime)s - %(message)s'
logging.basicConfig(filename=cfg.log_file, format=log_format, level=logging.INFO, force=True)

files = FileProcessing(local_process=True)
calls = ApiCalls(cfg)
        
def load_data(start_date: dt.date, end_date: dt.date) -> None:
    run     = calls.load_records(start_date, end_date, all_records = True)

    if run == False:
        logging.warning('The process completed as no data was found for the time period')
        return

    # Downloading all form information
    calls.get_form_data()

    run     = calls.load_records(start_date, end_date, all_records = False)

    start_time = dt.datetime.now()
    for index, record in  calls.df_eval_records.iterrows():
        calls.load_answers(record['recordId'], record['evaluation.id'])     
    logging.info(f'Time spent on evaluation details:    {dt.datetime.now()-start_time}')
    
    calls.load_agents(start_date)

    dataframe_list, dataframe_names = files.group_dfs(calls)
    
    # Once all dataframes are loaded, export them to individual parquet files.
    
    for item in range(0, len(dataframe_list)):
        files.export(dataframe_list[item], dataframe_names[item], start_date)
    
    logging.info('The process completed successfully')\

    cfg.configuration['general']['start_date'] = date_max.strftime('%Y-%m-%d')
    cfg.update(cfg.configuration)

if __name__ == '__main__':
    try:
        min_date:dt.date    = dt.datetime.strptime(cfg.start_date, '%Y-%m-%d').date()
        max_date:dt.date    = dt.date.today()

        relative_diff:relativedelta = relativedelta(max_date, min_date)

        month_iters:int = (relative_diff.years*12) + relative_diff.months
        day_iters:int   = relative_diff.days

        date_min = min_date

        while(month_iters!=0 or day_iters!=0):
            
            if month_iters != 0:
                date_diff = relativedelta(months=month_iters-1, days=day_iters)
                month_iters = month_iters - 1
            else:
                date_diff = relativedelta(days=day_iters-1)
                day_iters = day_iters - 1

 
            date_min = dt.datetime.strptime(str(date_min), '%Y-%m-%d').date()
            date_max = dt.date.today() - date_diff

            print(f'Max: {date_max} - Min: {date_min}')

            load_data(date_min, date_max)

            date_min = date_max
    
    except Exception as e:
        logging.exception(e)