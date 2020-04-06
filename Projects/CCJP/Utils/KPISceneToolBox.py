from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSceneToolBox
from Projects.CCJP.Data.LocalConsts import Consts

__author__ = 'nidhin'


class SceneToolBox:

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
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
        self.current_scene_fk = self.scene_info.iloc[0].scene_fk
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.store_info.iloc[0].store_fk
        self.store_type = self.data_provider.store_type
        self.kpi_static_data = common.get_kpi_static_data()
        self.match_display_in_scene = self.data_provider.match_display_in_scene
        self.scene_template_info = self.scif[['scene_fk',
                                              'template_fk', 'template_name']].drop_duplicates()

    def main_function(self):
        score = 0
        Consts.FACINGS_OF_POSM_IN_CELL
        return score
