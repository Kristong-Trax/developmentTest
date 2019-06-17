from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCBOTTLERSUS.Calculations import CCBOTTLERSUSCalculations

# if __name__ == '__main__':
#     LoggerInitializer.init('ccbottlersus calculations')
#     Config.init()
#     project_name = 'ccbottlersus'
#
#
#     sessions = [
#         'fe6e86a5-e96c-4ed1-b285-689ee8da393c',
#         'f6c0247d-64b4-4d11-8e0b-f7616316c08f',
#         'FAB57A4E-4814-4B74-A521-53A003864D06',
#         'BE9F0199-17B6-4A11-BA97-97751FE6EE0E'
#     ]
#
#     for session in sessions:
#         print('***********************************************************************************')
#         print('_______________________ {} ____________________'.format(session))
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         CCBOTTLERSUSCalculations(data_provider, output).run_project_calculations()
