from mock import MagicMock
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Output, KEngineDataProvider
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCUS.SceneKpis.SceneCalculations import SceneCalculations
from Projects.CCUS.Calculations import CCUSCalculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('KEngine')
    Config.init('KEngine')
    project_name = 'ccus'
    data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
    session_and_scenes = {
        # "00004868-bde7-4495-9d09-3036d1f26b5b": [863470, 863543, 863545],
        # "00006BEC-3629-4ED3-BCB9-0242A3C90472":	[1300604, 1300619, 1300647,	1300665, 1300683, 1300695, 1300704,	1300708, 1300720, 1300737, 1300754, 1300765, 1300783, 1300804, 1300848, 1300853, 1300877, 1300882, 1300920, 1300970, 1301017, 1301034],
        # "0000BD22-F95F-4A26-A9B0-70420C671AA3":	[914219, 914221, 914222, 914230, 914232, 914234, 914235],
        # "8395fc95-465b-47c2-ad65-6d10de13cd75":	{'scene1':[10474779, '88a6cdff-4215-4efd-a7a5-23da566ab62f'],
        #                                         'scene2':[10420017, '723eb38f-e241-4718-a8e1-0ff8d8cc1a1f']},
        'f6553bc2-a26c-472c-beef-e1bc7035394d': [],
        '7cd9609e-40cb-48fe-aade-04470bbcd082': [],  # Military
        'ecf47463-bf81-43da-8078-6d9c5caa074c': [],
        '97304f07-2ac1-4f1a-bb08-a57ca625aaa6': [],
        '92945c9b-ec85-404b-a7c8-dc5fcb3b7180': [],
        '0889cbd0-cfac-44a7-8ffb-ba68c3d45f02': [],
        '09981b36-5f78-4a84-ae81-82ba24b325b0': [],
        '0b2291ba-182c-466e-a826-601c1ac69786': [],
        '0d68c9ea-c681-4d08-b621-905240ce7946': [],
        '17ce7052-e0bf-4a34-90d7-0e3af3e6cf34': [],
        '19fddde6-b0de-4d02-8f68-8c218555d76e': [],
        '20e467b6-1a65-4eb5-9e09-c78d5cbce069': [],
        '384de0a7-fd12-4752-91e1-fc8a4419a229': [],
        '395e64ac-f36d-40c8-8a69-8ec4744b16c9': [],
        '4ce62c0e-7d0b-41ae-88f8-6f5e06b4f46f': [],
        '4f54a89d-e939-4801-9175-7b7e3f9f3b41': [],
        '5d227fe8-ea98-4ca2-bd99-1c0bf1c4bbd8': [],
        '5dc095de-f2e2-4be7-957c-dbf18e72cc06': [],
        '5e87b9b3-8e59-440d-bdf9-7595d26ac059': [],
        '6058c489-c50b-4495-b45f-98ab745b9d36': [],
        '613d6780-da25-405b-85b1-40b1884175c2': [],
        '718c5dde-3730-432c-aa90-c0392cc97442': [],
        '76b2275d-1d2d-493b-8690-aecbcf0804cb': [],
        '7cbfadc8-d133-4a7e-a91c-11e007eb622e': [],
        '89b4ab3e-08d4-4c34-839a-7a2184c27ef9': [],
        '8b78b7b4-0c2f-4017-831e-08f3f2422018': [],
        '8E65CD95-83BB-4526-9E7C-9A6A56F82017': [],
        '930b33cc-14a5-4f8e-99f6-18936b88b9f0': [],
        'a4edc5e2-06db-48ff-9e6d-957b830c5044': [],
        'ad1e1699-32ab-4ba6-9b1b-1cc747af0a38': [],
        'bba4fbff-a612-4f8f-819e-e8445f75d7e6': [],
        'c9c80318-e79f-4f40-9c04-783862f509c5': [],
        'd2908bbc-aacb-4331-a71e-ece17f13c317': [],
        'de481061-47b5-4ab3-8045-9d8c48ecd643': [],
        'ef0c4dc4-d94c-48d0-8f5e-201ca7ce6c5e': [],
        'fb82bbc9-354c-4b36-9b3a-8bd61ad05582': [],
    }

    for session in session_and_scenes.keys():
        print("==================== {} ====================".format(session))
        for scene in session_and_scenes[session]:
            data_provider.load_scene_data(session, session_and_scenes[session][scene][0],session_and_scenes[session][scene][1])
            output = VanillaOutput()
            SceneVanillaCalculations(data_provider, output).run_project_calculations()
            save_scene_item_facts_to_data_provider(data_provider, output)
            SceneCalculations(data_provider).calculate_kpis()

        data_provider.load_session_data(session)
        output = Output()
        CCUSCalculations(data_provider, output).run_project_calculations()
