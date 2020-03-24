
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCAAU.Calculations import Calculations


# if __name__ == '__main__':
#     LoggerInitializer.init('ccaau calculations')
#     Config.init()
#     project_name = 'ccaau'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = ['D172D47E-DEEC-4F7D-89AE-0A2C7DDF97EB'
#                 ]
#     for sess in sessions:
#         data_provider.load_session_data(sess)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
