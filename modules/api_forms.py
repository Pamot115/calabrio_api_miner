# Internal references
from modules.api_connection import ApiConnection
from modules.auxiliar import Config

# External libraries
import datetime as dt
import logging, math
import pandas as pd

class Forms():

    def __init__(self, configuration: Config) -> None:

        self.caller         = ApiConnection(configuration)
        self._headers       = self.caller.headers
        self._url           = self.caller.url

        # Form dataframes
        self.df_forms           = pd.DataFrame()
        self.df_form_sections   = pd.DataFrame()
        self.df_form_questions  = pd.DataFrame()
        self.df_form_options    = pd.DataFrame()

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