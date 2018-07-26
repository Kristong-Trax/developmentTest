
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.DIAGEODISPUS.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('diageodispus calculations')
#     Config.init()
#     project_name = 'diageodispus'
#     data_provider = KEngineDataProvider(project_name)
#     session = '0F857202-B810-43CB-B152-41B3045EE99E'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
