from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.BEIERSDORFRU.Calculations import Calculations
from Trax.Utils.Conf.Configuration import Config


if __name__ == '__main__':
    LoggerInitializer.init('beiersdorfru calculations')
    Config.init()
    project_name = 'beiersdorfru'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['B32CB617-7A7E-40CA-8B0E-A5C3061694F9']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
