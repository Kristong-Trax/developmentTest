
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSUAE.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('marsuae calculations')
    Config.init()
    project_name = 'marsuae'
    data_provider = KEngineDataProvider(project_name)
    session = '10643805-CFCB-419A-A415-FFC0BCBE96F6'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
