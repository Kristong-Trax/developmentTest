
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Projects.LIONNZ_SAND.Calculations import Calculations

if __name__ == '__main__':
    # LoggerInitializer.init('lionnz-sand calculations')
    Config.init()
    project_name = 'lionnz-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '29ffddd9-8114-4249-b9ec-76c2db070a9f',
        'abe00951-82b0-4e2b-942d-310a1ea04c14',
    ]
    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        print "*******************************"
