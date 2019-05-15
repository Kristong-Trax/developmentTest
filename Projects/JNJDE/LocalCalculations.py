
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JNJDE.Calculations import JNJDECalculations


if __name__ == '__main__':
    LoggerInitializer.init('jnjde calculations')
    Config.init()
    project_name = 'jnjde'
    data_provider = KEngineDataProvider(project_name)
    session = 'e31d1a3c-e190-4b50-843a-0a74cd44c5ab'
    data_provider.load_session_data(session)
    output = Output()
    JNJDECalculations(data_provider, output).run_project_calculations()
