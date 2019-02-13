#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.SANOFIKZ_SAND.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('sanofikz-sand calculations')
#     Config.init()
#     project_name = 'sanofikz-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'FB5B9FC3-9CBB-4452-93E1-CD5DB1F455EE'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
