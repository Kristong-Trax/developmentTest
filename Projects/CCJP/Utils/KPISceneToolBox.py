import os
import pandas as pd

from Trax.Utils.Logging.Logger import Log
from Projects.CCJP.Data.LocalConsts import Consts
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSceneToolBox

__author__ = 'nidhin'


class SceneToolBox(GlobalSceneToolBox):

    def __init__(self, data_provider, output):
        GlobalSceneToolBox.__init__(self, data_provider, output)
        self.data_provider = data_provider
        self.output = output
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider.scene_item_facts
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.current_scene_fk = self.scene_info.iloc[0].scene_fk
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.store_info.iloc[0].store_fk
        self.match_display_in_scene = self.data_provider.match_display_in_scene
        set_up_file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..',  'Data', 'setup.xlsx')
        self.set_up_file = pd.read_excel(set_up_file_name, sheet_name='Functional KPIs', keep_default_na=False)

    def main_function(self):
        sku_pos_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.FACINGS_OF_SKU_POSM_IN_CELL)
        display_pos_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.FACINGS_OF_DISPLAY_POSM_IN_CELL)
        self.calculate_facings_in_cell_per_product(kpi_fk=sku_pos_kpi_fk)
        self.calculate_count_posm_per_scene(kpi_fk=display_pos_kpi_fk)

    def calculate_count_posm_per_scene(self, kpi_fk):
        Log.info("Calculate scene level POSM facings count at scene level for session: {}".format(self.session_uid))
        if self.match_display_in_scene.empty:
            Log.info("No POSM detected at scene level for session: {} scene: {}".format(self.session_uid,
                                                                                        self.current_scene_fk))
            return False
        grouped_data = self.match_display_in_scene.groupby(['bay_number', 'display_fk'])
        for data_tup, scene_data_df in grouped_data:
            bay_number, display_fk = data_tup
            posm_count = len(scene_data_df)
            template_fk = self.scene_info['template_fk']
            if not template_fk.empty:
                cur_template_fk = int(self.scene_info['template_fk'])
            else:
                Log.error("{project}: Scene ID {scene} is not complete and not found in scene Info.".format(
                    project=self.project_name,
                    scene=self.current_scene_fk))
                continue
            Log.info(
                "Scene Level POSM pk {posm} has count {count} in bay {bay} for session: {sess} scene: {scene}".format(
                    posm=display_fk,
                    count=posm_count,
                    bay=bay_number,
                    sess=self.session_uid,
                    scene=self.current_scene_fk))
            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=display_fk,
                                           denominator_id=self.store_id,
                                           context_id=cur_template_fk,
                                           numerator_result=bay_number,
                                           result=posm_count,
                                           score=posm_count,
                                           by_scene=True,
                                           )

    def calculate_facings_in_cell_per_product(self, kpi_fk):
        Log.info("Calculate product level POSM facings count at scene level for session: {}".format(self.session_uid))
        pos_products = self.products.query(
            '(product_type=="POS")'
        )
        if pos_products.empty:
            Log.info("No POSM detected at session level for session: {} scene: {}".format(self.session_uid,
                                                                                          self.current_scene_fk))
            return False
        set_up_info = self.set_up_file[self.set_up_file[Consts.KPI_TYPE_COLUMN] == Consts.FACINGS_OF_SKU_POSM_IN_CELL]
        # Currently only stacking config is taken from config file
        stacking_str = set_up_info['Include Stacking'].iloc[0]
        match_prod_scene_data = pos_products.merge(
            self.match_product_in_scene, how='left', on='product_fk', suffixes=('', '_mpis'))
        if stacking_str.strip().lower() != 'include':
            # should exclude all stacked products - layer > 1. Layers negative are not stacked.
            Log.info(" **** Exclude stacked POS. Negatives are included. ****")
            match_prod_scene_data = match_prod_scene_data.query('stacking_layer<=1')
        grouped_data = match_prod_scene_data.groupby(
            ['bay_number', 'shelf_number', 'product_fk']
        )
        for data_tup, scene_data_df in grouped_data:
            bay_number, shelf_number, product_fk = data_tup
            facings_count_in_cell = len(scene_data_df)
            template_fk = self.scene_info['template_fk']
            if not template_fk.empty:
                cur_template_fk = int(self.scene_info['template_fk'])
            else:
                Log.error("{project}: Scene ID {scene} is not complete and not found in scene Info.".format(
                    project=self.project_name,
                    scene=self.current_scene_fk))
                continue
            Log.info("Session Level POSM pk {posm} has count {count} for session: {sess} scene: {scene}".format(
                posm=product_fk,
                count=facings_count_in_cell,
                sess=self.session_uid,
                scene=self.current_scene_fk))
            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=product_fk,
                                           denominator_id=scene_data_df.fillna(value=0).manufacturer_fk.iloc[0],
                                           context_id=cur_template_fk,
                                           numerator_result=bay_number,
                                           denominator_result=shelf_number,
                                           result=facings_count_in_cell,
                                           score=facings_count_in_cell,
                                           by_scene=True,)
