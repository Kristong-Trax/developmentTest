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
        '64CC70E0-3454-459B-8817-2BC78EDC4256',  # with data
        # 'B5A1D8EF-9A15-4B17-94C3-868CAC29FF0F',
        # '8BD94E57-821B-4C0B-91A0-F6DD3DBF8413'
    ]
    for session in sessions:
        print("Running for {}".format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
