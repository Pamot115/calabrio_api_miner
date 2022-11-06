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

        # Agent's data
        self.df_agents          = pd.DataFrame()
        self.df_schedules       = pd.DataFrame()

    def parallel_process(self, source_df: pd.DataFrame, split_size: int, method, process_name: str):
        # Internal references
        from modules.auxiliar import FileProcessing
        
        # External libraries
        from multiprocessing import Pool

        fp = FileProcessing(split_size)
        chunks = fp.split_chunks(source_df)

        logging.info(f'{len(chunks)} iterations will be required to process all {process_name}')

        pool = Pool(processes=16)
        for item in chunks:
            data = pool.map(method, [c for c in item])
        pool.close()
        
        merged_df = pd.DataFrame()
        
        for x in data:
            merged_df = pd.concat([merged_df, x], ignore_index = True)
        return merged_df

    def get_form_data(self) -> list[pd.DataFrame]:
        '''
            Downloads the base data for the evaluation form, then uses the
            expand_data method to navigate to the nested subtables.

            Returns a list of dataframes.
        '''
        try:
            start_time = dt.datetime.now()
            url_evalform = f'{self._url}/recording/evalform'
            
            json_forms      = self.caller.get(url_evalform)
            df_forms        = pd.json_normalize(json_forms)

            for item in json_forms:
                self.__load_form_sections(item)

            df_forms        = df_forms.rename(columns={'id': 'formId'})        
            self.df_forms   = df_forms
            
            logging.info(f'Time elapsed for form data:  {dt.datetime.now() - start_time}')
        except Exception as e:
            logging.exception(e)

    def __load_form_sections(self, json_forms: str) -> None:
        """
        Method that expands the Sections column contained within each item of the Forms JSON.
        
        This does not return data, instead, it appends it to a dataframe stored within the class (self.df_form_sections)
        
        Similar to get_form_data, this method will then iterate through each row and call __load_form_questions
        to expand the corresponding nested JSON.

        Args:
            * json_forms: str ->  JSON String for each row of the Forms dataframe.
        """
        try:
            # Expanding the 'Sections' data as it is a nested JSON
            json_sections   = json_forms['sections']
            df_sections     = pd.json_normalize(json_sections)
            df_sections['formId'] = json_forms['id']

            # Adding the form ID
            df_sections     = df_sections.rename(columns={'id': 'sectionId'})
            
            for index, row in df_sections.iterrows():
                self.__load_form_questions(row)

            self.df_form_sections   = pd.concat([self.df_form_sections, df_sections], ignore_index=True)
        except Exception as e:
            logging.exception(e)

    def __load_form_questions(self, df_sections: pd.Series) -> None:
        """
        Method that expands the Questions column contained within each row of the Sections dataframe.
        
        This does not return data, instead, it appends it to a dataframe stored within the class (self.df_form_questions)
        
        Similar to get_form_data, this method will then iterate through each row and call __load_form_options
        to expand the corresponding nested JSON.

        Args:
            * df_sections: pd.Series ->  Pandas Series that represents a row of the Sections dataframe.
        """
        try:
            # Expanding the 'Questions' data as it is a nested JSON
            json_questions   = df_sections['questions']
            df_questions     = pd.json_normalize(json_questions)

            # Adding previous IDs
            df_questions['formId'] = df_sections['formId']
            df_questions['sectionId'] = df_sections['sectionId']
            df_questions    = df_questions.rename(columns={'id': 'questionId'})
            
            for index, row in df_questions.iterrows():
                self.__load_form_options(row)

            self.df_form_questions   = pd.concat([self.df_form_questions, df_questions], ignore_index=True)
        except Exception as e:
            logging.exception(e)

    def __load_form_options(self, df_questions: str) -> None:
        """
        Method that expands the Options column contained within each row of the Questions dataframe.
        
        This does not return data, instead, it appends it to a dataframe stored within the class (self.df_form_options)

        Args:
            * df_questions: pd.Series ->  Pandas Series that represents a row of the Questions dataframe.
        """
        try:
            # Expanding the 'Sections' data as it is a nested JSON
            json_options    = df_questions['options']
            df_options      = pd.json_normalize(json_options)

            # Adding previous IDs
            df_options['formId']    = df_questions['formId']
            df_options['sectionId'] = df_questions['sectionId']
            df_options['questionId'] = df_questions['questionId']
            df_options  = df_options.rename(columns={'id': 'optionId'})
        
            self.df_form_options   = pd.concat([self.df_form_options, df_options], ignore_index=True)
        except Exception as e:
            logging.exception(e)

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

    def load_answers(self, record: int, evaluation: int) -> None:
        '''
        This method will iterate through the provided Evaluation ID and gather the answer for
        the questions and sections.
            Args:
                record_id       - Identifier of the recording
                evaluation_id   - Numeric value for the evaluation
        The arguments are obtained during the iteration of evaluated records.
        '''
        try:
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
        except Exception as e:
            logging.exception(e)

    def __load_eval_sections(self, json_evaluation: str, evaluation_id: int) -> None:
        try:
            # Expanding the 'Sections' data as it is a nested JSON
            json_sections   = json_evaluation['sections']
            df_sections     = pd.json_normalize(json_sections)

            # Adding the evaluation ID
            df_sections['evaluationId'] = evaluation_id

            self.df_eval_sections    = pd.concat([self.df_eval_sections, df_sections], ignore_index=True)

            self.__load_eval_questions(json_sections, evaluation_id)
        except Exception as e:
            logging.exception(e)

    def __load_eval_questions(self, json_sections: str, evaluation_id: int) -> None:
        try:
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
        except Exception as e:
            logging.exception(e)

    def __load_eval_comments(self, evaluation_url: str, evaluation_id: int) -> None:
        try:
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

        except Exception as e:
            logging.exception(e)

    def load_agents(self, schedule_date: str) -> None:
        '''
            This method will obtain all agents listed under Team 0 (All active agents).
        '''
        try:
            start_time = dt.datetime.now()

            self.schedule_date = schedule_date
            #  URL  to obtain the data (team 0 lists all agents for the organization)
            url = f'{self._url}/org/common/agents/for/team/0'
            json_agents = self.caller.get(url)

            # Reading only the agents from the team
            df_agents   = pd.json_normalize(json_agents['agents'])
            df_agents   = df_agents.rename(columns={'id': 'agentId'})
            self.df_agents  = pd.concat([self.df_agents, df_agents], ignore_index = True)
            
            df_schedules = self.parallel_process(df_agents['agentId'], 250, self._load_agents_schedule, 'schedules')
            self.df_schedules   = pd.concat([self.df_schedules, df_schedules], ignore_index = True)

            logging.info(f'Time elapsed for agents and schedules data:    {dt.datetime.now()-start_time}')

        except Exception as e:
            logging.exception(e)

    def _load_agents_schedule(self, agent_id: int) -> pd.DataFrame:
        '''
            This method will iterate through each given agent to obtain the planned schedule,
            and the actual time spent to verify the adherence.
            No data transformation is done.
                Args:
                    dfAgents    -   dataframe of agents, obtained from list_agents
                    schDays     -   number of days in the past from which the schedule will be obtained
        '''
        df_schedules = pd.DataFrame()
        try:
            #   Evaluate the date from which the schedule will be obtained
            schDate = self.schedule_date

            # for agent_id in df_agents['agentId']:
            # Iterate through the list of agents to obtain each schedule
            url = f'{self._url}/scheduling/adherence/agent/{agent_id}?date={schDate}'
            json_schedules = self.caller.get(url)

            id      = {"agentId":agent_id}
            json_schedules.update(id)
            json_schedules['scheduleDate'] = schDate
            
            df_tmp_schedule = pd.json_normalize(json_schedules)
            df_schedules = pd.concat([df_schedules, df_tmp_schedule], ignore_index=True)
            return df_schedules
        
        except Exception as e:
            logging.exception(e)