import pandas as pd
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
    project_name = 'ccbottlersus-sand'

    session = 'e7067c2d-1712-4dfd-8700-b1390f20cdd8'
    scenes = [817627, 524253, 524260, 524274, 524306, 524317,
              524330, 524338, 524343, 524354, 524398, 524420, 524445, 524494,
              ]

    session = '3FB3B1D2-237F-4CA0-8AFF-B68F6DCC16FF'
    scenes = [
                628916,
                628924,
                628934,
                628947,
                628951,
                628945,
                628949,
                628964,
                628967,
                628975
            ]


    session = 'ffe5750c-2ffe-42bd-96bc-7772d8d6b942'
    # scenes = [376377, 376378, 376379, 376381, 376383, 376385, 376399]

    # sw coke cooler sessions
    sessions = [
                 '3FB3B1D2-237F-4CA0-8AFF-B68F6DCC16FF',
                # 'E5D74460-D34C-413E-B886-F976535CE88D',
                # '2461329B-9115-4F70-BAF4-C6701355A343',
                # '337A7F53-9E39-4277-852E-2C7CF2FE3285',
                # 'C0692CCD-2373-466C-9510-DC692264C38A',

                 ]
    # # sw NTBA sessions
    sessions = [
                '8E0FCD9E-C20A-4A68-B24F-2037707C404B',
                # 'f8dcaeaf-9129-4621-ae8a-6a131ce3b15e',
                # 'e450ca7d-c90d-4164-bb60-1fc8f90d08e0',
                ]
    # for session in sessions:
    #     data_provider = KEngineDataProvider(project_name)
    #     data_provider.load_session_data(session)
    #     scif = data_provider['scene_item_facts']
    #     scenes = scif['scene_id'].unique().tolist()
    #
    #     for scene in scenes:
    #         print('scene')
    #         data_provider = KEngineDataProvider(project_name)
    #         data_provider.load_scene_data(session, scene)
    #         output = VanillaOutput()
    #         SceneVanillaCalculations(data_provider, output).run_project_calculations()
    #         save_scene_item_facts_to_data_provider(data_provider, output)
    #         SceneCalculations(data_provider).calculate_kpis()



    sessions = [
        "e7067c2d-1712-4dfd-8700-b1390f20cdd8"
    ]
    sessions = [
        # '6f963459-f5f1-4fc4-a77e-2a804a885f6b',
        # '76D3ED09-1B30-4D59-8EEE-F05D8478F607',
        # '413734a3-4e21-494d-bb18-418e5aacc4bd',
        # 'E83A6DE8-CB50-4299-BF90-05D58A22DE0C',
        # 'c2b54ac8-f018-468f-8d2b-f00ab246c985',
        '2BE54279-6758-4C0B-A8D0-EE7353B97560',
        ]

    sessions = [
        # 'ffe5750c-2ffe-42bd-96bc-7772d8d6b942',
        # 'f4311b59-74a0-4400-8b92-3d373dfb557a',
        # 'c2b54ac8-f018-468f-8d2b-f00ab246c985',
        # '3FB3B1D2-237F-4CA0-8AFF-B68F6DCC16FF'
        # 'fff64504-26e3-4ddd-b60c-098daab8caa1',
        # 'FF7EB0D2-DF3C-4971-BFA9-033E3144A194',
        # 'ffd9cc9d-9847-402d-9026-54a3bdf10a84',

        # 'ffe5750c-2ffe-42bd-96bc-7772d8d6b942'
        # '3FB3B1D2-237F-4CA0-8AFF-B68F6DCC16FF',

        'c2b54ac8-f018-468f-8d2b-f00ab246c985'
    ]

    # sw ratio sessions
    # sessions = [
    #             '6ce2258b-6a4a-4a85-ba18-6ead7cf1b772',
    #             '1ECD1661-27E1-4324-AEDB-BC3CBF44CACB',
    #             'cd059d85-46a6-4bf8-9e46-d2e96fbeac82',
    #             '50820afd-91e4-4367-af3d-f82d37d76a7c',
    #             ]
    # # sw NTBA sessions
    sessions = [
                '8E0FCD9E-C20A-4A68-B24F-2037707C404B',
                # 'f8dcaeaf-9129-4621-ae8a-6a131ce3b15e',
                # 'e450ca7d-c90d-4164-bb60-1fc8f90d08e0',
                ]
    # # sw Club Coke Chill Plus
    # sessions = [
                # 'e5f82e5c-58ea-4af1-9701-ff38bb6e65c1',
                # '1ECD1661-27E1-4324-AEDB-BC3CBF44CACB',
                # '900f9fdb-0f3c-4da1-855b-99afd1b6e006',
              #   'cd059d85-46a6-4bf8-9e46-d2e96fbeac82',
              # ]
    # # CMA
    # sessions = [
    #             'ffd9cc9d-9847-402d-9026-54a3bdf10a84',
    #             'fff64504-26e3-4ddd-b60c-098daab8caa1',
    #             'FF871FE8-C74A-4C6A-BCE3-42C9021D9C43',
    #             'ea9e5f71-1a43-4c44-9b99-4e0f7fa89068',
    #             ]
    # # sw coke cooler sessions
    sessions = [
              # '3FB3B1D2-237F-4CA0-8AFF-B68F6DCC16FF',
                # 'E5D74460-D34C-413E-B886-F976535CE88D',
                # '2461329B-9115-4F70-BAF4-C6701355A343',
                # '337A7F53-9E39-4277-852E-2C7CF2FE3285',
                'C0692CCD-2373-466C-9510-DC692264C38A',
                 ]
    # La Blob
    # sessions = [
    #     '3FB3B1D2-237F-4CA0-8AFF-B68F6DCC16FF',
    #     'E5D74460-D34C-413E-B886-F976535CE88D',
    #     '2461329B-9115-4F70-BAF4-C6701355A343',
    #     '337A7F53-9E39-4277-852E-2C7CF2FE3285',
    #     'C0692CCD-2373-466C-9510-DC692264C38A',
    #             'ffd9cc9d-9847-402d-9026-54a3bdf10a84',
    #             'fff64504-26e3-4ddd-b60c-098daab8caa1',
    #             'FF871FE8-C74A-4C6A-BCE3-42C9021D9C43',
    #             'ea9e5f71-1a43-4c44-9b99-4e0f7fa89068',
    #             'e5f82e5c-58ea-4af1-9701-ff38bb6e65c1',
    #             '1ECD1661-27E1-4324-AEDB-BC3CBF44CACB',
    #             '900f9fdb-0f3c-4da1-855b-99afd1b6e006',
    #             'cd059d85-46a6-4bf8-9e46-d2e96fbeac82',
    #             'e5f82e5c-58ea-4af1-9701-ff38bb6e65c1',
    #             '1ECD1661-27E1-4324-AEDB-BC3CBF44CACB',
    #             '900f9fdb-0f3c-4da1-855b-99afd1b6e006',
    #             'cd059d85-46a6-4bf8-9e46-d2e96fbeac82',
    #     '8E0FCD9E-C20A-4A68-B24F-2037707C404B',
    #     'f8dcaeaf-9129-4621-ae8a-6a131ce3b15e',
    #     'e450ca7d-c90d-4164-bb60-1fc8f90d08e0',
    #             '6ce2258b-6a4a-4a85-ba18-6ead7cf1b772',
    #             '1ECD1661-27E1-4324-AEDB-BC3CBF44CACB',
    #             'cd059d85-46a6-4bf8-9e46-d2e96fbeac82',
    #             '50820afd-91e4-4367-af3d-f82d37d76a7c',
    #     'c2b54ac8-f018-468f-8d2b-f00ab246c985'
    # ]

    for session in sessions:
        print('*************************************')
        print('~~~~~~~{}~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        CCBOTTLERSUS_SANDCalculations(data_provider, output).run_project_calculations()

