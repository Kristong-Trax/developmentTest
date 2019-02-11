import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.GPUS.Calculations import Calculations
from Projects.GPUS.SceneKpis.SceneCalculations import SceneCalculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('gpus calculations')
    Config.init()
    project_name = 'gpus'

    sessions = [
                # 'ff3f79ef-6d5e-4ea8-9a87-87ea60cbf629',
                'd78f0285-b4e2-4a75-831f-f7b4a81b6eef',
                # '040aac07-013b-4d6b-a408-a6348faf317d'
    ]

    # sessions = [
        # '0006173f-2110-4eb2-8010-70182ef0c510',

        # '0269d74a-c448-457b-aa67-ca7b1562bf51'
    # '08465b37-6157-49f2-9ffd-852c8a00553b',
    # '0ce7d05d-62e3-4f25-9e07-b02eeb5dab6f',
    # '13ab9667-c5f3-49d6-be85-7b0613864bf0',
    # '243afe24-14c7-425d-adac-2315c32e1cf2',
    # '276c31f5-95b7-4fd1-8f1d-62cf08f5b473',
    # '3a5d3658-80c3-4975-8181-1c5d17736179',
    # '450d2626-02e6-4407-897e-fe36f94b30bb',
    # '48452305-1e9e-401d-9c7d-b3c94e930680',
    # '4bda1666-55cc-4bde-aff2-d9b462c8fe01',
    # '5321f9f8-6b9c-49e0-bd51-46d1723b14d5',
    # '5fd29afe-1787-4d80-b5a5-4ed7afb074d6',
    # '68a65125-41d3-4eac-8437-13dadf6bc41b',
    # '6c605b37-5d2a-4eb6-a489-afd0f3e37491',
    # '7c8dcf9b-a8f7-48a7-aedd-ff7fe736d237',
    # '81f1441c-11db-4df1-82f3-aea1d072ee28',
    # 'a69943f6-5110-489b-86b5-7368e1bd2d72',
    # 'b94f8a1b-ea9e-4a62-aa18-3cc65d48441f',
    # 'bac09993-cb06-40c7-bfa6-f66dfedd51f1',
    # 'c0fb14ba-4a03-4e35-a816-5f8106bf0e46',
    # 'c256e3d5-aa1a-4418-a7d3-373813ac547d',
    # 'd78f0285-b4e2-4a75-831f-f7b4a81b6eef',
    # 'dd0a08c6-d6f2-4068-815a-f84ca56d42aa',
    # 'e88ec29b-19bd-417c-af66-75d333b789a6',
    # 'f1ab67aa-c862-4985-8538-8f535ca722a5',
    # 'fb59ed3f-2da6-4c61-8cf1-2c8f3a026e46',
    # 'ff3f79ef-6d5e-4ea8-9a87-87ea60cbf629'

    # ]

    sessions = [

        '4d86ef43-d9f9-448b-84d4-f7fc52c3ec80',
        # '19732daf-3494-4c0c-8b1d-1476a78ee7cc',
        # '05aae106-4fe2-4fd3-9b69-d6374c8810d9',
        # 'd1ed3cc3-dec6-4a95-afc5-f2858aeb1cc5',
        # '3901060b-5651-4ca2-b783-50df00fff54e',
        # '0a0dd274-f666-46b0-ad68-9805b76814c7',
        # '89f7db8b-accf-4177-b116-a2e90b95f3ec',
        # '1f652202-4eef-4d78-bfba-ef5c2f8913c7',
        # '0fcdba86-052e-4509-b40e-084a9467991f',
        # '2afdcc15-4ed1-4dcb-8708-4873f42834a3',

    ]

    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)

        scif = data_provider['scene_item_facts']
        scenes = scif['scene_id'].unique().tolist()

        # for scene in scenes:
        #     data_provider = KEngineDataProvider(project_name)
        #     data_provider.load_scene_data(session, scene)
        #     output = VanillaOutput()
        #     SceneVanillaCalculations(data_provider, output).run_project_calculations()
        #     save_scene_item_facts_to_data_provider(data_provider, output)
        #     SceneCalculations(data_provider).calculate_kpis()

        output = Output()
        Calculations(data_provider, output).run_project_calculations()

