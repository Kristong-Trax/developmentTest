
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PNGHK.Calculations import Calculations
from Projects.PNGHK.SceneKpis.SceneCalculations import SceneCalculations
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
import pandas as pd

__author__ = 'ilays'


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id',
                                     Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('pnghk calculations')
    Config.init()
    project_name = 'pnghk'
    data_provider = KEngineDataProvider(project_name)
    sessions = {
        # '7c64c9c4-a439-42af-8b23-2dea2f909f3d': ['185596'],
        #         '58f29898-f31f-4182-adb4-c8f1e5c51621': ['192809']}
        # '14f1ce60-b974-4649-b72b-06fd138c9f40': []}
        # '92653457-346d-4b00-bfd6-ceba4ab07ab1': [],
        # '3032a21c-4b47-45a6-ba60-aa2a6ae90730': [],
        # '4d0a507c-7ba4-49f6-9177-4b1c4fd4408c': [],
        # '74038BBC-A81F-4F77-B404-9C252781C927': [],
        # '8762738e-6104-4a28-b64a-82d46cb31585': [],
        # '90929e2d-47ef-494e-bfaa-40f4f9df9fef': [],
        # '3032a21c-4b47-45a6-ba60-aa2a6ae90730': [],
        # '4d0a507c-7ba4-49f6-9177-4b1c4fd4408c': [],
        # '74038BBC-A81F-4F77-B404-9C252781C927': [],
        # '8762738e-6104-4a28-b64a-82d46cb31585': [],
        # '90929e2d-47ef-494e-bfaa-40f4f9df9fef': [],
        # '92653457-346d-4b00-bfd6-ceba4ab07ab1': [],
        # '9ef5fb03-8ab0-4cc0-bc39-56679fe0142f': [],
        # '13dde5e5-01d4-4ca3-ab38-9c8c78c8de0e': [],
        '138122c6-815c-4b0a-9547-c7dc9eb7db60': []
    }


    for session in sessions:
        print "Running for {}".format(session)
        # for scene in sessions[session]:
        #     print('Calculating scene id: ' + str(scene))
        #     data_provider = KEngineDataProvider(project_name)
        #     data_provider.load_scene_data(session, scene)
        #     output = VanillaOutput()
        #     SceneVanillaCalculations(data_provider, output).run_project_calculations()
        #     save_scene_item_facts_to_data_provider(data_provider, output)
        #     SceneCalculations(data_provider).calculate_kpis()
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
