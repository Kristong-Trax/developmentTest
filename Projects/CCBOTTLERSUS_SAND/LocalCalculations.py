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
    project_name = 'ccbottlersus'

    sessions = [
                '6ce2258b-6a4a-4a85-ba18-6ead7cf1b772',
                '1ECD1661-27E1-4324-AEDB-BC3CBF44CACB',
                'cd059d85-46a6-4bf8-9e46-d2e96fbeac82',
                '50820afd-91e4-4367-af3d-f82d37d76a7c',
                ]

    # msc sessions
    sessions = [
        'fb150355-0844-4168-92e4-77062f37dac5',  # CR
        'ffbb7916-42b4-4876-acae-d7128c0c1747',  # LS
        'FE199EC1-03F9-4D59-BE74-112F76FF8EBA',
        'ff6bcdde-35b9-4925-beee-66829d048795',  # Drug
        'fca64a5b-b139-4c48-9e03-df5f0d5b7a9a',
        'ffc87836-bcb6-49b5-a6a5-7ca91e954e1e',  # Value
        'fb8ba95d-916a-4fe4-8064-c3b1e716ee85'
    ]

    # liberty sessions
    sessions = [
        'cdca16d6-6242-4b1d-b590-38eaaab8269f',
        '4af7543e-7804-4092-91e6-f896640098f7',
        'abf1c868-6bf5-4588-abfe-76257e7a13fe',
        'eedafa69-86fe-4323-9e75-d456fd6f280c'
    ]

    for session in sessions:
        print('*************************************************************************')
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~{}~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)

        scif = data_provider['scene_item_facts']
        scenes = scif['scene_id'].unique().tolist()

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

