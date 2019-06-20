from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCBOTTLERSUS.Calculations import CCBOTTLERSUSCalculations

# if __name__ == '__main__':
#     LoggerInitializer.init('ccbottlersus calculations')
#     Config.init()
#     project_name = 'ccbottlersus'
#
#     # MSC
#     sessions = [
#         'FC51FAC6-4EBB-4C9B-AC1B-F72052442DDE',
#         'E79B5B80-BAA2-4FA0-8C1F-594269B39457',
#         'E86F80DE-62C2-44AB-9949-80E520BCB3B2',
#         'F05079E5-11C4-4289-B5AE-5B8205594E15',
#         'dc322cc1-bfb7-4f2b-a6c3-c4c33a12b077'
#     ]
#
#     for session in sessions:
#         print('***********************************************************************************')
#         print('_______________________ {} ____________________'.format(session))
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         CCBOTTLERSUSCalculations(data_provider, output).run_project_calculations()
