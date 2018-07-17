#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.PERFETTICN.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('perfetticn calculations')
#     Config.init()
#     project_name = 'perfetticn'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'AC3ED6D6-19E5-4B1A-A215-E4B28A659118'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
