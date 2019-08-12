#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.GSKJP_SAND.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('gskjp-sand calculations')
#     Config.init()
#     project_name = 'gskjp-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'a2a637c4-b5ae-40d7-a1d3-81c6847a15d3'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
