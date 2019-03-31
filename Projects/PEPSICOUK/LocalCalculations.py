
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PEPSICOUK.Calculations import Calculations
from Projects.PEPSICOUK.SceneKpis.SceneCalculations import PEPSICOUKSceneCalculations


if __name__ == '__main__':
    LoggerInitializer.init('pepsicouk calculations')
    Config.init()
    project_name = 'pepsicouk'
    # project_name = 'diageous-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '5aeb5ce9-6d1e-499d-8b9c-c736b5ad82cd'
    data_provider.load_session_data(session)
    # output = Output()
    # Calculations(data_provider, output).run_project_calculations()
    scenes = data_provider.scenes_info.scene_fk.tolist()
    for scene in scenes:
        data_provider.load_scene_data(session, scene)
        # output = VanillaOutput()
        # SceneVanillaCalculations(data_provider, output).run_project_calculations()
        # save_scene_item_facts_to_data_provider(data_provider, output)
        PEPSICOUKSceneCalculations(data_provider).calculate_kpis()
