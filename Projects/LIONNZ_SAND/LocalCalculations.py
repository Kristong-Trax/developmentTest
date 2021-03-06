
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
        '5281C4DF-8FD9-41E1-84A2-76179B95CA25',
    ]
    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        print "*******************************"
