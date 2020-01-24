from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CARLSBERG.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('carlsberg calculations')
    Config.init()
    project_name = 'sinopacificTH'  # 'carlsberg'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '020951DF-D8C6-4439-A8B2-0E18702548A1',
        ]
    for session in sessions:
        print("Running for {}".format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
