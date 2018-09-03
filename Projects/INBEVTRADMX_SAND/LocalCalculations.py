#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.INBEVTRADMX_SAND.Calculations import INBEVTRADMX_SANDCalculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('inbevtradmx-sand calculations')
#     Config.init()
#     project_name = 'inbevtradmx-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'fed83d98-6b1f-478a-ae98-7f594d0a0c9c'
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVTRADMX_SANDCalculations(data_provider, output).run_project_calculations()
