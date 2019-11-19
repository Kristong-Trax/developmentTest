
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
        'B50F4323-2B87-4D68-BB16-D2DB91FAFB0C',  # '2019-11-08'
        '5FBE05E6-EB53-4C0F-8250-19CC3FEA801C',  # '2019-11-13'
        '48FD19E3-6F46-494C-89AF-ED41AED20106',  # '2019-11-13'
        'C03F39B0-E9B7-49BE-9F59-EF28C4E5477F',  # '2019-11-13'
        'C14C9A61-FFFB-4A25-9F7B-F87F833FDCDF',  # '2019-11-13'


    ]
    for session in sessions:
        Log.info("Running for session >>>> {}".format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
