# Internal references
from modules.api_connection import ApiConnection
from modules.auxiliar import Config

# External libraries
import datetime as dt
import logging, math
import pandas as pd

class Evaluations():

    def __init__(self, configuration: Config) -> None:

        self.caller         = ApiConnection(configuration)
        self._headers       = self.caller.headers
        self._url           = self.caller.url

        # Evaluation dataframes
        self.df_eval_details    = pd.DataFrame()
        self.df_eval_sections   = pd.DataFrame()
        self.df_eval_questions  = pd.DataFrame()
        self.df_eval_comments   = pd.DataFrame()

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