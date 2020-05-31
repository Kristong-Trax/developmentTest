

from Projects.PS1_SAND.Calculations import DIAGEOTWCalculations
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


if __name__ == '__main__':
    LoggerInitializer.init('ps1-sand calculations')
    Config.init()
    project_name = 'ps1-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '70b69c85-61a6-4ce4-ad60-f0d174ced645'
    data_provider.load_session_data(session)
    output = Output()
    DIAGEOTWCalculations(data_provider, output).run_project_calculations()
