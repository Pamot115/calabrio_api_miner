# Internal references
from modules.api_connection import ApiConnection
from modules.auxiliar import Config

# External libraries
import datetime as dt
import logging, math
import pandas as pd

class Records:
    '''
        This Class contains the methods to pull the data through different Calabrio API calls,
        each method returns at least one Pandas Dataframe.
    '''
    def __init__(self, configuration: Config):
        self.caller         = ApiConnection(configuration)
        self._headers       = self.caller.headers
        self._url           = self.caller.url

        # Contact dataframes
        self.df_all_records     = pd.DataFrame()
        self.df_eval_records    = pd.DataFrame()

    def load_records(self, date_start: dt.date, date_end: dt.date = dt.date.today(), all_records: bool = True) -> tuple[bool, pd.DataFrame]:
        '''
            This method will iterate through all interactions and their corresponding evaluation data.
                Args:
                    days_start    - Number of days from today to the evaluation date range start
                    days_end      - Number of days from today to the evaluation date range end
                    all_records   - Defines whether all records will be pulled, or only those with an evaluation
        '''
        try:
            start_time = dt.datetime.now()

            #   Query parsing elements depending on the allRecords arg
            if all_records:
                _date_query = f'beginDate={date_start}&endDate={date_end}'
                _limit = '&limit=500000'
            else:
                _date_query = f'beginDate=2020-01-01&endDate={date_end}'
                _date_evaluation_range = f'&dateEvaluatedStart={date_start}&dateEvaluatedEnd={date_end}'
                _limit = '&limit=50000'

            #   Standard parsing elements (do not modify)
            _reason             = '&reason=recorded'
            _search_scope       = '&searchScope=allEvaluations'
            _metadata           = '&expand=metadata'
            _event_calculations = '&expand=eventCalculations'

            #   Assemble the query for the URL
            if all_records:
                query = (
                    _date_query + _search_scope + _reason + _metadata +
                    _event_calculations + _limit
                )
            else:
                query = (
                    _date_query + _date_evaluation_range + _search_scope + _reason + _metadata +
                    _event_calculations + _limit
                )
            
            url = f'{self._url}/recording/contact?{query}&searchStats=true'

            # Evaluate numer of records within the specified time frame
            call_count = self.caller.get(url)['count']

            # Once evaluated, determine if the entire process should run or not
            if int(call_count) == 0:
                logging.warning(f'''Filtering between {date_start} - {date_end} returned no records.
                Refer to {url}''')
                data_found = False
                df_records = pd.DataFrame()
                return data_found, df_records
            else:
                logging.info(f'''Filtering between {date_start} - {date_end} shows a total of {call_count} record(s).
                Refer to {url}''')
                data_found = True

            url = f'{self._url}/recording/contact?{query}'

            df_records = pd.json_normalize(self.caller.get(url))
            df_records = df_records.rename(columns={'id': 'recordId'})

            dt_columns = ['startTime', 'evaluation.evaluated']

            #   Data formatting
            for column in dt_columns:
                if column in df_records.columns:
                    df_records[column] = pd.to_datetime(df_records[column], unit='ms').apply(
                        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else math.nan)

            #   Remove duplicate records
            df_records = df_records.drop_duplicates(subset='recordId', keep="last")
            
            if all_records:
                logging.info(f'Time elapsed for all records:    {dt.datetime.now()-start_time}')
                self.df_all_records = df_records
            else:
                logging.info(f'Time elapsed for evaluations:    {dt.datetime.now()-start_time}')
                self.df_eval_records = df_records

            return data_found
        except Exception as e:
            logging.exception(e)