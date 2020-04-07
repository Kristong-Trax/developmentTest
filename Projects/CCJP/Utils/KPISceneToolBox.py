from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSceneToolBox
from Trax.Utils.Logging.Logger import Log
from Projects.CCJP.Data.LocalConsts import Consts

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
        self.store_type = self.data_provider.store_type
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.match_display_in_scene = self.data_provider.match_display_in_scene
        self.scene_template_info = self.scif[['scene_fk',
                                              'template_fk', 'template_name']].drop_duplicates()

    def main_function(self):
        sku_pos_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.FACINGS_OF_SKU_POSM_IN_CELL)
        display_pos_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.FACINGS_OF_DISPLAY_POSM_IN_CELL)
        self.calculate_facings_in_cell_per_product(kpi_fk=display_pos_kpi_fk)
        self.calculate_count_posm_per_scene(kpi_fk=sku_pos_kpi_fk)

    def calculate_count_posm_per_scene(self, kpi_fk):
        Log.info("No POSM detected at scene level for session: {}".format(self.session_uid))
        if self.match_display_in_scene.empty:
            Log.info("No POSM detected at scene level for session: {}".format(self.session_uid))
            return False
        grouped_data = self.match_display_in_scene.groupby(['display_fk'])
        for display_fk, scene_data_df in grouped_data:
            posm_count = len(scene_data_df)
            template_fk = self.scene_info[self.scene_info['scene_fk'] == self.current_scene_fk].get('template_fk')
            if not template_fk.empty:
                cur_template_fk = int(template_fk)
            else:
                Log.info("{project}: Scene ID {scene} is not complete and not found in scene Info.".format(
                    project=self.project_name,
                    scene=self.current_scene_fk))
                continue
            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=display_fk,
                                           denominator_id=self.store_id,
                                           context_id=cur_template_fk,
                                           result=posm_count,
                                           score=posm_count
                                           )

    def calculate_facings_in_cell_per_product(self, kpi_fk):
        match_prod_scene_data = self.match_product_in_scene.merge(
            self.all_products, how='left', on='product_fk', suffixes=('', '_prod'))
        grouped_data = match_prod_scene_data.query(
            '(stacking_layer==1) or (product_type=="POS")'
        ).groupby(
            ['bay_number', 'shelf_number', 'product_fk']
        )
        for data_tup, scene_data_df in grouped_data:
            print "************"
            print scene_data_df.fillna(value=0).manufacturer_fk.iloc[0]
            bay_number, shelf_number, product_fk = data_tup
            facings_count_in_cell = len(scene_data_df)
            cur_template_fk = int(self.scene_info[
                                      self.scene_info['scene_fk'] == self.current_scene_fk].get('template_fk'))
            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=product_fk,
                                           denominator_id=scene_data_df.fillna(value=0).manufacturer_fk.iloc[0],
                                           context_id=cur_template_fk,
                                           numerator_result=bay_number,
                                           denominator_result=shelf_number,
                                           result=facings_count_in_cell,
                                           score=facings_count_in_cell)
