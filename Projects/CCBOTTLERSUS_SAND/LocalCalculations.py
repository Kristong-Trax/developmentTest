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

    ''' impulse zone cooler '''
    # sessions = [
    #             '6ce2258b-6a4a-4a85-ba18-6ead7cf1b772',
    #             '1ECD1661-27E1-4324-AEDB-BC3CBF44CACB',
    #             'cd059d85-46a6-4bf8-9e46-d2e96fbeac82',
    #             '50820afd-91e4-4367-af3d-f82d37d76a7c',
    #             ]
    #
    ''' sw ratio sessions '''
    # sessions = [
    #             '6ce2258b-6a4a-4a85-ba18-6ead7cf1b772',
    #             '1ECD1661-27E1-4324-AEDB-BC3CBF44CACB',
    #             'cd059d85-46a6-4bf8-9e46-d2e96fbeac82',
    #             '50820afd-91e4-4367-af3d-f82d37d76a7c',
    #             ]
    ''' sw NTBA sessions '''
    # sessions = [
    #             '8E0FCD9E-C20A-4A68-B24F-2037707C404B',
    #             'f8dcaeaf-9129-4621-ae8a-6a131ce3b15e',
    #             'e450ca7d-c90d-4164-bb60-1fc8f90d08e0',
    #             ]
    ''' sw Club Coke Chill Plus '''
    # sessions = [
    #             'e5f82e5c-58ea-4af1-9701-ff38bb6e65c1',
    #             '1ECD1661-27E1-4324-AEDB-BC3CBF44CACB',
    #             '900f9fdb-0f3c-4da1-855b-99afd1b6e006',
    #             'cd059d85-46a6-4bf8-9e46-d2e96fbeac82',
    #           ]
    # ''' CMA '''
    sessions = [
                'ffd9cc9d-9847-402d-9026-54a3bdf10a84',
                'fff64504-26e3-4ddd-b60c-098daab8caa1',
                'FF871FE8-C74A-4C6A-BCE3-42C9021D9C43',
                'ea9e5f71-1a43-4c44-9b99-4e0f7fa89068',
                ]
    ''' sw coke cooler sessions '''
    # sessions = [
    #           '3FB3B1D2-237F-4CA0-8AFF-B68F6DCC16FF',
    #             # 'E5D74460-D34C-413E-B886-F976535CE88D',
    #             # '2461329B-9115-4F70-BAF4-C6701355A343',
    #             # '337A7F53-9E39-4277-852E-2C7CF2FE3285',
    #             # 'C0692CCD-2373-466C-9510-DC692264C38A',
    #              ]
    

    for session in sessions:
        print('*************************************************************************')
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~{}~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)

        # scif = data_provider['scene_item_facts']
        # scenes = scif['scene_id'].unique().tolist()
        #
        # for scene in scenes:
        #     print('scene')
        #     data_provider = KEngineDataProvider(project_name)
        #     data_provider.load_scene_data(session, scene)
        #     output = VanillaOutput()
        #     SceneVanillaCalculations(data_provider, output).run_project_calculations()
        #     save_scene_item_facts_to_data_provider(data_provider, output)
        #     SceneCalculations(data_provider).calculate_kpis()

        output = Output()
        CCBOTTLERSUS_SANDCalculations(data_provider, output).run_project_calculations()

