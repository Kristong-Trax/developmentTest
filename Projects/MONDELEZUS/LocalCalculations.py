import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MONDELEZUS.Calculations import Calculations



# if __name__ == '__main__':
#     LoggerInitializer.init('mondelezus calculations')
#     Config.init()
#     project_name = 'mondelezus'
#
#     sessions = [
#         '8cf2b7e2-ee78-4179-98d2-a316e7c43c1c',
#         # '66432d9e-8253-4d63-962c-4e25e2ccd9b3',
#
#                 ]
#
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()

