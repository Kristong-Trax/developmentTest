#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.PEPSICORU.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('pepsicoru calculations')
#     Config.init()
#     project_name = 'pepsicoru'
#     data_provider = KEngineDataProvider(project_name)
#     session = '75067fdd-25ac-434e-970a-f0fc0060510b'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
