
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.SINGHATH.Calculations import Calculations


if __name__ == '__main__':
    Config.init()
    project_name = 'singhath-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        'a70cf08d-ab91-4136-9b0e-dab5a15153c1',
        '6561D6CE-C995-4032-8568-3F3ACCC306CD'
    ]
    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        print "*******************************"
