
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.LIONNZ.Calculations import Calculations


if __name__ == '__main__':
    # LoggerInitializer.init('lionnz-sand calculations')
    Config.init()
    project_name = 'lionnzdev'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '6561D6CE-C995-4032-8568-3F3ACCC306CD'
    ]
    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        print "*******************************"
