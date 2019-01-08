
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JNJDE.Calculations import JNJDECalculations


if __name__ == '__main__':
    LoggerInitializer.init('jnjde calculations')
    Config.init()
    project_name = 'jnjde'
    data_provider = KEngineDataProvider(project_name)
    session = '8b058146-8985-45c8-b55d-8c33908fc308'
    data_provider.load_session_data(session)
    output = Output()
    JNJDECalculations(data_provider, output).run_project_calculations()
