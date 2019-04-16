
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
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)

if __name__ == '__main__':
    LoggerInitializer.init('pnghk calculations')
    Config.init()
    project_name = 'pnghk'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        'bd812815-649d-4553-8fee-e3e39a9174f7',
        'aefaf595-d0ac-40a8-8fa5-1f4b1763ed2a',
        '5a7faa1c-3a69-47a8-aac4-19133bb64549',
        '6cccc451-19b5-42d2-a581-1dcdd5b02491',
        '4f43c5ec-c6c9-4139-ac19-55334d17be74',
        '41a4abd4-0ee7-490a-868b-9043f162d252',
        '8e687c04-ac58-47a4-8000-bd8afd9382d0',
        '6dc13c96-5943-42f8-ad26-48f9e5617f0f',
        # 'ed5bc391-ce81-4d7e-b16d-1de5a573cbe0',  # has smart probe/69066 has hanger probe/69044 has stock
        # 'ad052993-9609-4aba-a927-dbaaff036280',  # has smart
        # '6c56a073-c8db-42aa-a491-c38b0c1c4086',  # has smart
        # 'dca92352-a549-4956-88b0-0811b35137c3',
    ]
    for session in sessions:
        print "Running for {}".format(session)
        # for scene in [66586]:
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

