
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS_SAND2.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous-sand2 calculations')
    Config.init()
    project_name = 'diageous-sand2'
    sessions = ['5F4800CC-0DC3-44D9-BA01-43A7FF4D712C']
    for i, session in enumerate(sessions):
        print (i, session)
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
