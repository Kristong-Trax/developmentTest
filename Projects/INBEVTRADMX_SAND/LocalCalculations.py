#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.INBEVTRADMX_SAND.Calculations import INBEVTRADMXCalculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('inbevtradmx-sand calculations')
#     Config.init()
#     project_name = 'inbevtradmx-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'fffff255-d0f6-4cdd-bf1f-81c1df465e8f'
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVTRADMXCalculations(data_provider, output).run_project_calculations()
