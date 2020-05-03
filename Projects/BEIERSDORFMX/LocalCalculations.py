from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.BEIERSDORFMX.Calculations import Calculations
from Trax.Utils.Conf.Configuration import Config


if __name__ == '__main__':
    LoggerInitializer.init('beiersdorfmx calculations')
    Config.init()
    project_name = 'beiersdorfmx'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['BCBFB338-5CE2-42AA-BE10-2550D3F1589F', '30B0D4B9-6A2C-459F-B174-0FE511D8A0BD',
                    'EB4DDF0B-F3FF-4C82-96E0-A15948E40185', '13ec2133-1b81-40ce-97a5-570fd72e6e09']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
