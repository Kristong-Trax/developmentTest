
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Projects.LIONNZ.Calculations import Calculations
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

if __name__ == '__main__':
    LoggerInitializer.init('lionnz calculations')
    Config.init()
    project_name = 'lionnz'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        # '5281C4DF-8FD9-41E1-84A2-76179B95CA25',
        'F8D22FE7-2F35-4CE8-B457-9D14BC11A06A'
    ]
    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        print "*******************************"
