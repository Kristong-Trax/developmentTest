from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.SANOFIML.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('Kengine')
    Config.init()
    project_name = 'sanofiml'
    data_provider = KEngineDataProvider(project_name)
    session = '9e5cc77e-c3b7-48b8-a874-4d7d9f761d9c'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
