
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.INBEVTRADMX_SAND.Calculations import INBEVTRADMX_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('inbevtradmx-sand calculations')
    Config.init()
    project_name = 'inbevtradmx'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        'e077d60c-eb83-490e-b453-c6a93e46f04d'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        INBEVTRADMX_SANDCalculations(data_provider, output).run_project_calculations()
