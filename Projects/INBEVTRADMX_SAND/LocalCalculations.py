
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.INBEVTRADMX_SAND.Calculations import INBEVTRADMX_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('inbevtradmx-sand calculations')
    Config.init()
    project_name = 'inbevtradmx-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        'fffff255-d0f6-4cdd-bf1f-81c1df465e8f',
        'ffff7738-30f5-49d1-be22-628a127e2cd3',
        'ffff37c4-684d-4295-9d97-681808a753e4',
        'FFFF327A-B215-4E31-BE35-EC8A05DF61F0',
        '5efb7901-9d5b-4732-8448-c0c0791c69ec',
        '4dde9f5d-b0b5-4288-b5cb-4cc6330d82db'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        INBEVTRADMX_SANDCalculations(data_provider, output).run_project_calculations()
