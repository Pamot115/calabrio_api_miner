# Internal references
from modules.api_connection import ApiConnection
from modules.auxiliar import Config

# External libraries
import datetime as dt
import logging, math
import pandas as pd

class Agents():

    def __init__(self, configuration: Config) -> None:
        self.cfg        = configuration
        self.caller     = ApiConnection(configuration)
        self._headers   = self.caller.headers
        self._url       = self.caller.url

        # Agent's data
        self.df_agent_data          = pd.DataFrame()
        self.df_agent_schedules     = pd.DataFrame()

    def load_agents(self, schedule_date: str) -> None:
        '''
            This method will obtain all agents listed under Team 0 (All active agents).
        '''

        # Internal references
        from modules.auxiliar import FileProcessing

        fp = FileProcessing(self.cfg)

        try:
            start_time = dt.datetime.now()

            self.schedule_date = schedule_date
            #  URL  to obtain the data (team 0 lists all agents for the organization)
            url = f'{self._url}/org/common/agents/for/team/0'
            json_agents = self.caller.get(url)

            # Reading only the agents from the team
            df_agents   = pd.json_normalize(json_agents['agents'])
            df_agents   = df_agents.rename(columns={'id': 'agentId'})
            self.df_agent_data  = pd.concat([self.df_agent_data, df_agents], ignore_index = True)
            
            df_schedules = fp.parallel_process(df_agents['agentId'], 250, self._load_agents_schedule, 'schedules')
            self.df_agent_schedules   = pd.concat([self.df_agent_schedules, df_schedules], ignore_index = True)

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