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
#     session = '032ed290-b23e-4e14-bacc-13c084aacf6a'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
