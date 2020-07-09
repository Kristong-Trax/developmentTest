

# from Projects.PS2_SAND.SceneKpis.SceneCalculations import SceneCalculations
from Projects.CBCIL_SAND.Calculations import CBCILCalculations
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
# # from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
# import pandas as pd
#
# #
# def save_scene_item_facts_to_data_provider(data_provider, output):
#     scene_item_facts_obj = output.get_facts()
#     if scene_item_facts_obj:
#         scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
#     else:
#         scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
#     scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
#     data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('Simon')
    Config.init()
    project_name = 'ps2-sand'
    data_provider = KEngineDataProvider(project_name)
    # session = '4673efb5-31b1-436f-8d3b-52071290f091'
    session ='ff1902f4-470f-49a3-acbd-e3aab4753ffe'
    data_provider.load_session_data(session)
    output = Output()
    CBCILCalculations(data_provider, output).run_project_calculations()
#     scenes = data_provider.scenes_info.scene_fk.tolist()
#     for scene in scenes:
#         data_provider.load_scene_data(session, scene)
#         output = VanillaOutput()
#         SceneVanillaCalculations(data_provider, output).run_project_calculations()
#         save_scene_item_facts_to_data_provider(data_provider, output)
#         SceneCalculations(data_provider).calculate_kpis()


