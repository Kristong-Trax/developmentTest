
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKSG_SAND.Calculations import Calculations


# if __name__ == '__main__':
#     LoggerInitializer.init('gsksg-sand calculations')
#     Config.init()
#     project_name = 'gsksg-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '9d1b0810-b5af-11e8-b598-120384147dcc'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
