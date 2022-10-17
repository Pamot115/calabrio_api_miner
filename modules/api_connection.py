from modules.auxiliar import Config
import requests

class ApiConnection:
    '''
        This class creates the cookie with session_id used to retrieve from Calabrio's API.
    '''

    def __init__(self, configuration: Config):
        self.url = configuration.api_url
        
        url_auth = f'{self.url}/authorize'           # Authentication URL

        __credentials = dict(
            userId   = configuration.api_user,       # ID of the tenant’s user
            password = configuration.api_password,   # User’s password
            locale   = 'en'                          # User’s language. Default = en
        )

        session_id = requests.request('POST', url_auth, json=__credentials).json()['sessionId']
        self.headers = dict(cookie=f'hazelcast.sessionId={session_id}')
        
        # Save cookie for the session, as this will be used in all calls
        configuration.configuration['session'] = self.headers

        configuration.update(configuration.configuration)
    def get(self, url) -> str:
        '''
            This Method is to test the outcome from Calabrio of the URL provided
            Args URL(str)
        '''
        json_data = requests.request('GET', url, headers=self.headers).json()
        return json_data

