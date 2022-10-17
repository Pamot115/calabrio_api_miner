# Internal references
from modules.api_connection import ApiConnection
from modules.auxiliar import Config

# External libraries
import datetime as dt
import logging, math
import pandas as pd
import requests

class ApiCalls:
    '''
        This Class contains the methods to pull the data through different Calabrio API calls,
        each method returns at least one Pandas Dataframe.
    '''
    def __init__(self, configuration: Config):
        self.caller = ApiConnection(configuration)
        self.headers = self.caller.headers
        self._url     = self.caller.url

    def expand_data(self, input_df: pd.DataFrame, base_column: str, type: int) -> pd.DataFrame:
        '''
            Takes a dataframe and a nested columns to expand its content and append the source
            column id.

            This method is only being used to expand the evaluation form data.

            Args:
                input_df:     - pd.Dataframe  - The dataframe that contains the nested column
                base_column:  - The column with nested JSON data
                type:         - An index for the data to use, these are manually mapped.
        '''
        df_output = pd.DataFrame()

        for index, row in input_df.iterrows():
            tmp_df = pd.DataFrame(row[base_column])

            if type == 0:
                tmp_df['form_id'] = row['id']
            elif type == 1:
                tmp_df['form_id'] = row['form_id']
                tmp_df['section_id'] = row['id']
            elif type == 2:
                tmp_df['form_id'] = row['form_id']
                tmp_df['section_id'] = row['section_id']
                tmp_df['question_id'] = row['id']
            
            df_output = pd.concat([df_output, tmp_df], ignore_index=True)
        return df_output

    def get_form_data(self) -> list[pd.DataFrame]:
        '''
            Downloads the base data for the evaluation form, then uses the
            expand_data method to navigate to the nested subtables.

            Returns a list of dataframes.
        '''

        url_evalform = f'{self._url}/recording/evalform'

        df_forms: pd.DataFrame     = pd.json_normalize(self.caller.get(url_evalform))
        df_sections: pd.DataFrame  = self.expand_data(df_forms       , 'sections'  , 0)
        df_questions: pd.DataFrame = self.expand_data(df_sections    , 'questions' , 1)
        df_options: pd.DataFrame   = self.expand_data(df_questions   , 'options'   , 2)

        output = [df_forms, df_sections, df_questions, df_options]

        return output

    def load_records(self, days_start: int, days_end: int = 0, all_records: bool = True) -> tuple[bool, pd.DataFrame]:
        '''
            This method will iterate through all interactions and their corresponding evaluation data.
                Args:
                    days_start    - Number of days from today to the evaluation date range start
                    days_end      - Number of days from today to the evaluation date range end
                    all_records   - Defines whether all records will be pulled, or only those with an evaluation
        '''

        date_start = (dt.date.today() - dt.timedelta(days=days_start)).isoformat()
        date_end   = (dt.date.today() - dt.timedelta(days=days_end)).isoformat()

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
            df_records[column] = pd.to_datetime(df_records[column], unit='ms').apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else math.nan)

        #   Remove duplicate records
        df_records = df_records.drop_duplicates(subset='recordId', keep="last")
        
        return data_found, df_records