
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.LIONNZ.Calculations import Calculations


if __name__ == '__main__':
    Config.init()
    project_name = 'lionnz'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '58664d85-865c-4b10-adc1-cd7b67367fff',
        '02e04d93-5b92-4e04-b1a3-54c6fba20bb7',
        'f6430600-faea-41e1-b0ea-8c2e61ee4028',
    ]
    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        print "*******************************"
