from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.SANOFIML.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('sanofiml calculations')
    Config.init()
    project_name = 'sanofiml'
    data_provider = KEngineDataProvider(project_name)
    session = '9CF0B856-0D61-4456-8F2A-AC588457FD13'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
