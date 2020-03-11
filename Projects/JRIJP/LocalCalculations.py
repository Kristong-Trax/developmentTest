
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
        "3C868048-A67A-434C-BF6F-7834A40C88F9",
        "24084FC4-388A-416C-9D39-FB7065C7F3D2",
        "61AA5528-457D-499F-A1EF-3FCD309B1F51"
    ]

    for session in sessions:
        print "Running for {}".format(session)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
