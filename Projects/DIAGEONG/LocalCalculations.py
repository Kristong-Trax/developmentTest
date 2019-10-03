
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEONG.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageong calculations')
    Config.init()
    project_name = 'diageong'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['8dd865fc-6bc6-4745-aa14-f8eb27874101']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
