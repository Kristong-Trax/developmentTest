
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.SINOTH_SAND.Calculations import Calculations


if __name__ == '__main__':
    project_name = 'sinoth-sand'
    LoggerInitializer.init('{} Local Calculations'.format(project_name.upper()))
    Config.init()
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '17CA2DDF-B0C7-4AB1-A9EA-84542016DB36',  # '2017-04-10'
        'A08F663E-CCF7-4912-AA5B-CC8ED335B13C',  # '2017-04-06'

    ]
    for session in sessions:
        print "Running for {}".format(session)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
