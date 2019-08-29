
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.TNUVAILV2.Calculations import Calculations
from Trax.Utils.Conf.Configuration import Config


if __name__ == '__main__':
    LoggerInitializer.init('tnuvailv2 calculations')
    Config.init()
    project_name = 'tnuvailv2'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['236c1577-0ecb-4bf9-88b9-c9e87ab17c58']
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
