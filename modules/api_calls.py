# Internal references
from modules.api_connection import ApiConnection
from modules.auxiliar import Config

# External libraries
import datetime as dt
import logging, math
import pandas as pd

class ApiCalls:
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

        # Form dataframes
        self.df_forms           = pd.DataFrame()
        self.df_form_sections   = pd.DataFrame()
        self.df_form_questions  = pd.DataFrame()
        self.df_form_options    = pd.DataFrame()

        # Evaluation dataframes
        self.df_eval_details    = pd.DataFrame()
        self.df_eval_sections   = pd.DataFrame()
        self.df_eval_questions  = pd.DataFrame()
        self.df_eval_comments   = pd.DataFrame()
        
    def expand_data(self, input_df: pd.DataFrame, base_column: str, type: int) ->  None:
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

        start_time = dt.datetime.now()
        url_evalform = f'{self._url}/recording/evalform'

        df_forms        = pd.json_normalize(self.caller.get(url_evalform))
        df_sections     = self.expand_data(df_forms       , 'sections'  , 0)
        df_questions    = self.expand_data(df_sections    , 'questions' , 1)
        df_options      = self.expand_data(df_questions   , 'options'   , 2)

        df_forms        = df_forms.rename(columns={'id': 'formId'})
        df_sections     = df_sections.rename(columns={'id': 'sectionId'})
        df_questions    = df_questions.rename(columns={'id': 'questionId'})
        df_options      = df_options.rename(columns={'id': 'optionId'})

        self.df_forms           = df_forms
        self.df_form_sections   = df_sections
        self.df_form_questions  = df_questions
        self.df_form_options    = df_options
        
        logging.info(f'Time elapsed for form data:  {dt.datetime.now() - start_time}')

    def load_records(self, date_start: dt.date, date_end: dt.date = dt.date.today(), all_records: bool = True) -> tuple[bool, pd.DataFrame]:
        '''
            This method will iterate through all interactions and their corresponding evaluation data.
                Args:
                    days_start    - Number of days from today to the evaluation date range start
                    days_end      - Number of days from today to the evaluation date range end
                    all_records   - Defines whether all records will be pulled, or only those with an evaluation
        '''

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

    def load_answers(self, record: int, evaluation: int) -> None:
        '''
        This method will iterate through the provided Evaluation ID and gather the answer for
        the questions and sections.
            Args:
                record_id       - Identifier of the recording
                evaluation_id   - Numeric value for the evaluation
        The arguments are obtained during the iteration of evaluated records.
        '''
        # Defining the evaluation URL
        url = f'{self._url}/recording/contact/{record}/eval/{evaluation}'

        # Loading evaluation's data
        json_evaluation = self.caller.get(url)
        df_evaluation   = pd.json_normalize(json_evaluation)
       
        # Appending the Evaluation ID to each dataframe
        df_evaluation   = df_evaluation.rename(columns={'id': 'evaluationId'})
        
        self.df_eval_details  = pd.concat([self.df_eval_details, df_evaluation], ignore_index=True)

        self.__load_eval_sections(json_evaluation, evaluation)
        self.__load_eval_comments(url, evaluation)

    def __load_eval_sections(self, json_evaluation: str, evaluation_id: int) -> None:
         # Expanding the 'Sections' data as it is a nested JSON
        json_sections   = json_evaluation['sections']
        df_sections     = pd.json_normalize(json_sections)

        # Adding the evaluation ID
        df_sections['evaluationId'] = evaluation_id

        self.df_eval_sections    = pd.concat([self.df_eval_sections, df_sections], ignore_index=True)

        self.__load_eval_questions(json_sections, evaluation_id)

    def __load_eval_questions(self, json_sections: str, evaluation_id: int) -> None:
        # Loading question answers as these are contained by a nested json within the 'Sections' data
        df_questions      = pd.DataFrame()

        for item in json_sections:
            tmp_df      = pd.json_normalize(item['questions'])
            tmp_df['sectionId'] = item['id']
            tmp_df      = tmp_df.rename(columns={'id': 'questionId'})
            df_questions    = pd.concat([df_questions, tmp_df], ignore_index=True)

        # Adding the evaluation ID
        df_questions['evaluationId'] = evaluation_id

        self.df_eval_questions   = pd.concat([self.df_eval_questions, df_questions], ignore_index=True)

    def __load_eval_comments(self, evaluation_url: str, evaluation_id: int) -> None:
         # Finally, loading the comments for the evaluation
        url = f'{evaluation_url}/comment'
        json_comments   = self.caller.get(url)

        if len(json_comments) == 0:
            return

        df_comments = pd.json_normalize(json_comments)
        
        # Cleaning column data
        df_comments['$ref']     = df_comments['$ref'].str.replace(pat=r'^.*?comment/', repl='', regex=True)
        df_comments['created']  = pd.to_datetime(df_comments['created'], unit='ms').apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else math.nan)
        
        df_comments = df_comments.rename(columns={'$ref': 'commentId'})

        # Adding the evaluation ID
        df_comments['evaluationId'] = evaluation_id

        self.df_eval_comments    = pd.concat([self.df_eval_comments, df_comments], ignore_index=True)
