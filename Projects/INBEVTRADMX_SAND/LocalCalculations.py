#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.INBEVTRADMX_SAND.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('inbevtradmx-sand calculations')
#     Config.init()
#     project_name = 'inbevtradmx-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '10aee05a-0d9a-4f93-ba8e-90efd4d5c1dc'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
