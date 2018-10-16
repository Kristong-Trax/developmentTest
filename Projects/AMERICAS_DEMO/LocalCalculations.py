#
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.AMERICAS_DEMO.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('americas-demo calculations')
    Config.init()
    project_name = 'americas-demo'
    data_provider = KEngineDataProvider(project_name)
    session = 'd01ae84b-570b-4ee0-924e-0d2d3932f66f'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
#     test
# do somethihg
# do somethihg 2
# do somthing 3
# do somthing 4