from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.BATRU.Calculations import BATRUCalculations


if __name__ == '__main__':
    LoggerInitializer.init('KEngine')
    Config.init()

    project_name = 'batru'
    data_provider = KEngineDataProvider(project_name)
    sessions = [

        '7345E20D-99D8-4499-91D9-26BCF249AF38'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        BATRUCalculations(data_provider, output).run_project_calculations()
