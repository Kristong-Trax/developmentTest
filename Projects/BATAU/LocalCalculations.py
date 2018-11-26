
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.BATAU_SAND.Calculations import Calculations


# if __name__ == '__main__':
#     LoggerInitializer.init('batau-calculations')
#     Config.init()
#     project_name = 'batau'
#     data_provider = KEngineDataProvider(project_name)
#     #session = '1707F91D-E4A6-401B-86C8-E04AA942BCA8'
#     session = '557077e4-b393-4284-91a7-340cc07c6db0'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()