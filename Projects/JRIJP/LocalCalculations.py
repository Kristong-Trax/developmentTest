
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JRIJP.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('jrijp calculations')
    Config.init()
    project_name = 'jrijp'
    data_provider = KEngineDataProvider(project_name)

    sessions = [
        'B38407B6-FA49-44F3-BD98-EEBC6F3B0F3F',
        # 'D1DF72CD-FD45-41C4-B947-466D2786FCD5',
        # '652DC9E4-6437-4EED-9829-622BC9EC6890',
        # '9EEA0E8C-5F5C-4FB9-834A-EF7A4532B281',
    ]

    for session in sessions:
        print "Running for {}".format(session)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
