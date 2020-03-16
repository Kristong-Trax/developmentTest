from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CARLSBRGHK.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('carlsberg calculations')
    Config.init()
    project_name = 'carlsbrghk'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        'B6FB5E83-BBBF-4988-A60D-2A5B62EA8CC5'
        # '4DAFCE77-82FA-4A51-B2D3-AD24D561237E',  # 0 msl cooler
        # '64CC70E0-3454-459B-8817-2BC78EDC4256',  # with data
    ]
    for session in sessions:
        print("Running for {}".format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
