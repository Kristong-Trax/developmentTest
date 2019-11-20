
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Logging.Logger import Log
from Projects.SINOTH.Calculations import Calculations


if __name__ == '__main__':
    project_name = 'sinoth'
    LoggerInitializer.init('{} Local Calculations'.format(project_name.upper()))
    Config.init()
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '5AF0EA7B-DF60-4D9F-BC96-933FC7646C26',  # 15
        'C03F39B0-E9B7-49BE-9F59-EF28C4E5477F',  # 13
        'CAF4AEBD-5B7F-485C-B96E-FED0489BDF66',  # 12
        'E2E0C987-0575-47E0-8912-8F00983923F3'  # 12
    ]
    for session in sessions:
        Log.info("Running for session >>>> {}".format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
