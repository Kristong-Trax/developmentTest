
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.CCIT_SAND.Calculations import Calculations
# from Projects.CCIT_SAND.SceneKpis.SceneCalculations import SceneCalculations


# if __name__ == '__main__':
#     LoggerInitializer.init('ccit-sand calculations')
#     Config.init()
#     project_name = 'ccit-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '96AFB0D3-5D74-43DD-8D80-31F9C5CD3D07'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
    # scenes = [10, 11]
    # for scene in scenes:
    #     data_provider.load_scene_data(session, scene)
    #     SceneCalculations(data_provider).calculate_kpis()