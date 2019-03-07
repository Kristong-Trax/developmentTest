
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.NESTLEUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('nestleus calculations')
    Config.init()
    project_name = 'nestleus'
    data_provider = KEngineDataProvider(project_name)
    session = 'a27d1d20-a9ac-4bdf-9b68-ee5e4195224a'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
