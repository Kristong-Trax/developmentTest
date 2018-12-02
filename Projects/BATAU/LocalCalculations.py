#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.BATAU.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('batau-calculations')
#     Config.init()
#     project_name = 'batau'
#     data_provider = KEngineDataProvider(project_name)
#     #session = '1707F91D-E4A6-401B-86C8-E04AA942BCA8'
#     #session = '557077e4-b393-4284-91a7-340cc07c6db0'
#     #session = '734f6c43-99e0-4d43-982e-075ad967badb'
#     session = 'D64DE51A-AF7E-45D7-88C6-BA5C58C2FF8A'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()