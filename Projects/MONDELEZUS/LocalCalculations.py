# import pandas as pd
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.MONDELEZUS.Calculations import Calculations
#
#
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('mondelezus calculations')
#     Config.init()
#     project_name = 'mondelezus'
#
#     sessions = [
#         'ff2f7e32-7126-4084-b0a1-8da135994001',
#         '4018ac2a-6ff5-4180-ae45-87d7fbdbe75a',
#
#                 ]
#
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
#
