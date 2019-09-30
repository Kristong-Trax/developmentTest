
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PEPSICOUK.Calculations import PEPSICOUKCalculations
from Projects.PEPSICOUK.SceneKpis.SceneCalculations import PEPSICOUKSceneCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
import pandas as pd
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
# from datetime import datetime


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('pepsicouk calculations')
    Config.init()
    project_name = 'pepsicouk-sand'
    data_provider = KEngineDataProvider(project_name)
    # session = '470e2eb1-9031-46c1-8699-c47e18c58b9c'
    # session = 'e6bf953a-1d8c-4dd1-b558-f88ef1cb13e5'# results in DB
    # session = 'ed42eb37-7e91-4b4b-acd6-28af30dc68ee' # for full bay - results in db
    # session = '99577826-6687-4c65-8bc8-e13d65fd90b4' # for block, results in DB scene abn session
    # session = '10ebc650-2b9c-4031-ab1d-a96ccae13032' # for stack - results in db
    # session = '58272289-5f9a-4fb6-b8e0-a7f3bfa2e07c' # session with DF error on _set_scif
    # session = '8699c793-8a71-4a18-b24c-3785146075c4'
    # session = 'aa9d2ad8-61d5-4167-a4f1-e753ef366b39'
    session = '56d070bb-36ad-4590-bf83-4161d68cdb3c'
    data_provider.load_session_data(session)
    # output = Output()
    # PEPSICOUKCalculations(data_provider, output).run_project_calculations()
    # # scenes = data_provider.scenes_info.scene_fk.tolist()
    scenes = [64294]
    for scene in scenes:
        data_provider.load_scene_data(session, scene)
        output = VanillaOutput()
        SceneVanillaCalculations(data_provider, output).run_project_calculations()
        save_scene_item_facts_to_data_provider(data_provider, output)
        # t0=datetime.now()
        PEPSICOUKSceneCalculations(data_provider).calculate_kpis()
        # t1=datetime.now()
        # print 'scene_calc_time', t1-t0