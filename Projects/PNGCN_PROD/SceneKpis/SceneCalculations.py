import pandas as pd
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass
from Projects.PNGCN_PROD.KPISceneGenerator import SceneGenerator
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log



class SceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(SceneCalculations, self).__init__(data_provider)
        self.data_provider = data_provider
        self.scene_generator = SceneGenerator(self._data_provider)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.common = Common(self.data_provider)
        self.matches_from_data_provider = self.data_provider[Data.MATCHES]
        # self._monitor = None
        # self.timer = self._monitor.Timer('Perform', 'Init Session')

    def calculate_kpis(self):
        # self.timer.start()
        self.save_nlsos_to_custom_scif()
        self.scene_generator.scene_share_of_display()
        # self.timer.stop('KPIGenerator.run_project_calculations')

    def save_nlsos_to_custom_scif(self):
        matches = self.matches_from_data_provider.copy()
        mask = (matches.status != 2) & (matches.bay_number != -1) & (matches.shelf_number != -1) & \
               (matches.stacking_layer != -1) & (matches.facing_sequence_number != -1)
        matches_reduced = matches[mask]

        # calculate number of products in each stack
        items_in_stack = matches.loc[mask, ['scene_fk','bay_number', 'shelf_number', 'facing_sequence_number']].groupby(
            ['scene_fk','bay_number', 'shelf_number', 'facing_sequence_number']).size().reset_index()
        items_in_stack.rename(columns={0: 'items_in_stack'}, inplace=True)
        matches_reduced = matches_reduced.merge(items_in_stack, how='left',
                                                on=['scene_fk','bay_number', 'shelf_number', 'facing_sequence_number'])

        matches_reduced['w_split'] = 1 / matches_reduced.items_in_stack

        matches_reduced['gross_len_split_stack_old'] = matches_reduced['width_mm'] * matches_reduced.w_split

        matches_reduced['gross_len_split_stack_new'] = matches_reduced['width_mm_advance'] * matches_reduced.w_split
        new_scif_gross_split = matches_reduced[['product_fk','scene_fk',
                                'gross_len_split_stack_old','width_mm','gross_len_split_stack_new',
                                'width_mm_advance']].groupby(by=['product_fk','scene_fk']).sum().reset_index()
        new_scif = pd.merge(self.scif, new_scif_gross_split, how='left',on=['scene_fk','product_fk'])
        new_scif = new_scif.fillna(0)
        self.insert_data_into_custom_scif(new_scif)

    def insert_data_into_custom_scif(self, new_scif):
        session_id = self.data_provider.session_id
        new_scif['session_id'] = session_id
        delete_query = """DELETE FROM pservice.custom_scene_item_facts WHERE scene_fk = {}""".format(new_scif['scene_fk'].values[0])
        insert_query = """INSERT INTO pservice.custom_scene_item_facts \
                            (session_fk, scene_fk, product_fk, in_assortment_osa, length_mm_custom) VALUES """
        for i, row in new_scif.iterrows():
            insert_query += str(tuple(row[['session_id', 'scene_fk',
                                           'product_fk', 'in_assort_sc','gross_len_split_stack_new']])) + ", "
        insert_query = insert_query[:-2]
        # try:
        #     self.common.execute_custom_query(delete_query)
        # except:
        #     Log.error("Couldn't delete old results from custom_scene_item_facts")
        #     return
        try:
            self.common.execute_custom_query(insert_query)
        except:
            Log.error("Couldn't write new results to custom_scene_item_facts and deleted the old results")


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


# if __name__ == '__main__':
#     LoggerInitializer.init('pngcn calculations')
#     Config.init()
#     project_name = 'pngcn-prod'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'cb2cc33d-de43-4c35-a25b-ce538730037e'
#
#     scenes = [15140013, ]
#     for scene in scenes:
#         data_provider.load_scene_data(session, scene)
#         output = VanillaOutput()
#         SceneVanillaCalculations(data_provider, output).run_project_calculations()
#         save_scene_item_facts_to_data_provider(data_provider, output)
#         SceneCalculations(data_provider).calculate_kpis()
