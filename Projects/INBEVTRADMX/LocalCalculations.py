#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.INBEVTRADMX.Calculations import INBEVTRADMXCalculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('inbevtradmx calculations')
#     Config.init()
#     project_name = 'inbevtradmx'
#     data_provider = KEngineDataProvider(project_name)
#     session = '23bbacb3-4e48-424a-94c5-39e34523d008'
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVTRADMXCalculations(data_provider, output).run_project_calculations()
