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
        "71728e9c-f58a-49cc-9d44-5935d8040dcf",
        "f5c93d46-cab9-4295-8bda-a6b86d7e095b",
        "aca3dc10-c2a0-4fa6-9051-591bc6f74ac9",
        "9b27735f-cd29-42b1-87d2-09c1323da21a",
        "d3025ed7-9508-4540-9ad8-9898faa57c64",
        "71740d9f-dbc6-4e76-88f7-2d224dca534a",
        "c6516767-392b-489b-bd70-f840cf2f0351",
        "ecf2084f-3f01-4edd-ae3c-2a78de33ea60",
        "ac35736a-9947-4cc0-a54a-0afc3b4b1b95",
        "2e3ae4c7-beba-4c65-9b49-5d22fed3f2c3",
        "9285bbe6-458e-48e2-936e-26c76686f51a",
        "1EFFC14B-CD92-4235-B2B3-743858ACAB08",
        "ca186a4e-6378-4269-b333-a47d3fc9d462",
        "11006a86-31ed-4579-a48d-d91964e35b6a",
        "157086fd-1be9-46df-9d75-6c2990e290ec",
        "7455ef5b-1340-4ae1-b6ba-96eeda80885d",
        "2C00C628-C89D-45A8-AF39-B6D5055DFC72",
        "0fdcb37d-d7d4-453d-b3da-a8dc7a3f11ef",
        "63ee7243-30cf-4a93-a199-a1de8856059b",
        "5657c8f8-b6d2-4b17-abb6-9cbb288a5594",
        "1daedaf6-4da0-4ab9-bfdc-c2c4f13440f7",
        "830f4210-8119-45df-b8b7-56714fbfa999",
        "E4FAA01B-36BA-4CC6-B384-119C34958074",
        "7248681e-5b6a-4a79-bcef-2be712569501",
        "C20CEC79-5CDD-48CB-BFCE-3BAFF4245A62",
        "fc620f3c-2928-4c34-b19b-b959e1299184",
        "3c359054-301d-4bc6-8820-037258a118cd",
        "50FBB7A3-12E5-4B02-8EF9-89D92F8ED188",
        "ce279084-9c17-4719-bc14-3777728a0183",
        "b524e8de-8b9e-49eb-96f2-bf6689e64e5d",
        "C16C261B-E23C-4623-B70A-C279C76EA2AD",
        "dde7bb25-d32d-44fe-b541-243e22b36bcf",
        "37EE89EB-C1CD-4661-8307-D9DC053A6FC4",
        "3a3cc10c-be72-41d7-95aa-3ab9d4bfc4da",
        "ffdd7097-6081-4a50-9cea-2739d647341d",
        "ce806488-783e-4df7-b5af-fb6f802a8f8d",
        "30003B8E-25C0-4326-9E93-D8C417FF1AB2"
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

