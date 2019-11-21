
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Logging.Logger import Log
from Projects.SINOTH_SAND.Calculations import Calculations


if __name__ == '__main__':
    project_name = 'sinoth'
    LoggerInitializer.init('{} Local Calculations'.format(project_name.upper()))
    Config.init()
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '36951936-c432-4b48-a92c-9bbe916accf7',
        '3e1fd576-b31b-49d7-873c-2f52ade7d93d',
        'b8dde4fe-bc78-4692-9161-f2a78d291b33',
    ]
    for session in sessions:
        Log.info("Running for session >>>> {}".format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
