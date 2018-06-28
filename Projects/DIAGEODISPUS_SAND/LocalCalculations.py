
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEODISPUS_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageodispus-sand calculations')
    Config.init()
    project_name = 'diageodispus'
    data_provider = KEngineDataProvider(project_name)
    session = 'c531e6fb-1e5d-4369-b8ae-13d3bcb2e0a7'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
