
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
        'C03F39B0-E9B7-49BE-9F59-EF28C4E5477F'
    ]
    for session in sessions:
        Log.info("Running for session >>>> {}".format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
