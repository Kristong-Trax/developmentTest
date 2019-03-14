from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.BATRU.Calculations import BATRUCalculations


if __name__ == '__main__':
    LoggerInitializer.init('BATRU calculations')
    Config.init()
    project_name = 'batru'
    data_provider = KEngineDataProvider(project_name)
    sessions = \
        [
            'ff9876f5-cae4-45ae-9c43-802bfe757673',
            'fe4a5172-fe4a-4bdb-8148-955c7fa715b2',
            'fd746de2-bdcf-4cae-8b23-8d607d924186'
        ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        BATRUCalculations(data_provider, output).run_project_calculations()
