
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Projects.LIONNZ_SAND.Calculations import Calculations
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

if __name__ == '__main__':
    LoggerInitializer.init('lionnz-sand calculations')
    Config.init()
    project_name = 'lionnz-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '944A33D9-125B-4B6C-A628-DF3630C3FE19',
    ]
    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        print "*******************************"
