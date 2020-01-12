
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.TNUVAILV2.Calculations import Calculations
from Trax.Utils.Conf.Configuration import Config


if __name__ == '__main__':
    LoggerInitializer.init('tnuvailv2 calculations')
    Config.init()
    project_name = 'tnuvailv2'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['af905367-fc67-419e-aeaf-1634f7986dac']
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
