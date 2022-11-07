# Internal dependencies
from modules.api_agents import Agents
from modules.api_evaluations import Evaluations
from modules.api_forms import Forms
from modules.api_records import Records
from modules.auxiliar import Config, FileProcessing

# External libraries
from typing import Tuple
import datetime as dt
import logging
import pandas as pd

cfg = Config('config.yaml')
files = FileProcessing(cfg)

class ApiCaller():
    def __init__(self, start_date: dt.date, end_date: dt.date) -> None:
        self.start_date = start_date
        self.end_date   = end_date

        self.agents      = Agents(cfg)
        self.evaluations = Evaluations(cfg)
        self.forms       = Forms(cfg)
        self.records     = Records(cfg)

        self.instances   = [self.agents, self.evaluations, self.forms, self.records]

    def load_data(self) -> None:
        run     = self.records.load_records(self.start_date, self.end_date, all_records = True)

        if run == False:
            logging.warning('The process completed as no data was found for the time period')
            return

        run     = self.records.load_records(self.start_date, self.end_date, all_records = False)

        start_time = dt.datetime.now()
        for index, record in self.records.df_eval_records.iterrows():
            self.evaluations.load_answers(record['recordId'], record['evaluation.id'])     
        logging.info(f'Time spent on evaluation details:    {dt.datetime.now()-start_time}')
        
        # Downloading all form information
        self.forms.get_form_data()

        self.agents.load_agents(self.start_date)
    
    def export_data(self) -> None:
        dataframe_list: list[pd.DataFrame]  = list()
        dataframe_names: list[str]          = list()

        for item in self.instances:
            keys = list(item.__dict__.keys())
    
            df_names = [i for i in keys if i.startswith('df_')]
            df_objects = [getattr(item, i) for i in df_names]

            dataframe_list.extend(df_objects)
            dataframe_names.extend(df_names)

        # Once all dataframes are loaded, export them to individual parquet files.
        for item in range(0, len(dataframe_list)):
            files.export(dataframe_list[item], dataframe_names[item], self.start_date)
        
        logging.info('The process completed successfully')\

        cfg.configuration['general']['start_date'] = self.end_date.strftime('%Y-%m-%d')
        cfg.update(cfg.configuration)