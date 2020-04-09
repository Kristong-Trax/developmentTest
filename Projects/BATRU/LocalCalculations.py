from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.BATRU.Calculations import BATRUCalculations


if __name__ == '__main__':
    LoggerInitializer.init('BATRU calculations')
    Config.init()

    project_name = 'batru-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['ffe79c33-ca54-40c1-854b-535458ce567b']
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        BATRUCalculations(data_provider, output).run_project_calculations()
