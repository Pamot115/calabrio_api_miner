# Internal references
from modules.api_caller import ApiCaller
from modules.auxiliar import Config

# External libraries
from dateutil.relativedelta import relativedelta
import datetime as dt
import logging
import pandas as pd

cfg     = Config('config.yaml')

# Logging format and configuration file
log_format = '%(asctime)s - %(message)s'
logging.basicConfig(filename=cfg.log_file, format=log_format, level=logging.INFO, force=True)


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

            caller  = ApiCaller(date_min, date_max)
            caller.load_data()
            caller.export_data()

            date_min = date_max
    
    except Exception as e:
        logging.exception(e)