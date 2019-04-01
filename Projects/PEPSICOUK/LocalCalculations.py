
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
    # session = '470e2eb1-9031-46c1-8699-c47e18c58b9c'
    session = '10e09fd5-3537-48ba-85ca-eb14ab421400'
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
