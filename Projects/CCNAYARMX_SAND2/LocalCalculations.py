
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCNAYARMX_SAND2.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccnayarmx-sand2 calculations')
    Config.init()
    project_name = 'ccnayarmx-sand2'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['b8e4649b-ad07-4d35-95bd-78743ea47066']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
