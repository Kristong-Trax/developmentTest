
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Projects.GSKAU_SAND.Calculations import Calculations


if __name__ == '__main__':
    Config.init()
    project_name = 'gskau-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['00FCB53C-9BC5-4896-AF00-60A14C775D93',
                '0ce85bee-bddd-420d-9c72-7a4601be4f67',
                ]
    for session in sessions:
        print "*****************"
        print "*****************"
        print "running for session {}".format(session)
        print "*****************"
        print "*****************"
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
