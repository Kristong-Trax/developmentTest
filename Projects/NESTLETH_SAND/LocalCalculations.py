
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.NESTLETH_SAND.Calculations import Calculations
from Projects.NESTLETH_SAND.SceneKpis.SceneCalculations import SceneCalculations

# if __name__ == '__main__':
#     LoggerInitializer.init('nestleth-sand calculations')
#     Config.init()
#     project_name = 'nestleth-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'b2c89c48-26c0-4a8a-818a-f5b1cf7d62dd'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
# #scenes calculation
#     scenes = [2, 32, 49]
#     for scene in scenes:
#         data_provider.load_scene_data(session, scene)
#         SceneCalculations(data_provider).calculate_kpis()
