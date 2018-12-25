
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PNGHK_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('pnghk-sand calculations')
    Config.init()
    project_name = 'pnghk-sand'
    data_provider = KEngineDataProvider(project_name)
    data_provider = KEngineDataProvider(project_name)
    sessions = ['e9d4bf08-8aa1-4840-871f-a15643dda368','69426997-1E74-4DB2-9695-09DCE1EF58E3']
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
