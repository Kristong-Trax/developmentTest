
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.CCBOTTLERSUS_SAND.Calculations import CCBOTTLERSUS_SANDCalculations
from Projects.CCBOTTLERSUS_SAND.SceneKpis.SceneCalculations import SceneCalculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('ccbottlersus calculations')
    Config.init()
    project_name = 'ccbottlersus'
    sessions = [
        "12dfda9a-fce4-45b7-8b9f-140390f55e74",
        "fe63af52-6b87-4c94-82aa-9aee63116d45",
        "b93d6582-cee9-47e2-8c23-19a5e87683b1",
        "bc8a1244-a53d-4b2f-9591-fcf57f9d3054",
        "D3D3E61E-F595-4D9D-9B5F-7188321420E1",
        "546AAB1A-259D-4EDA-909E-D8AC9E89D4AD",
        "11044558-fc7f-4882-8243-e301528aa5e8",
        "402bb0f7-7e58-4532-94a5-21ed2538d2e6",
        "15283f33-65f7-4abf-91e2-084801ec4c61",
        "c2b4723f-ea1b-456d-9647-48ef779cfcb8",
        "9e0cd962-74b6-48ac-ba13-6e674c198ea3",
        "86997b82-e7e4-4155-91f5-9cd30de7b55c",
        "7c5284d4-93e0-46e2-a31c-75075d2323e0",
        "55d5c959-cb08-477b-9f9e-0af4fa9f3795",
        "714f5168-b9d9-4f9b-8f3e-3a0723c68253",
        "95260A0C-5C37-4675-9718-6144D31A040D"
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        CCBOTTLERSUS_SANDCalculations(data_provider, output).run_project_calculations()
    session = 'D7794D02-C46D-4A36-8D2E-6CDEBB443919'
    scenes = [97, 98, 99]
    for scene in scenes:
        data_provider.load_scene_data(session, scene)
        output = VanillaOutput()
        SceneVanillaCalculations(data_provider, output).run_project_calculations()
        save_scene_item_facts_to_data_provider(data_provider, output)
        SceneCalculations(data_provider).calculate_kpis()
