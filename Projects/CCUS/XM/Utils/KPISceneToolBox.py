from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from Projects.CCUS.XM.Utils.Const import Const

__author__ = 'Shivi'


class CCUSSceneToolBox:

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.scene_type = self.scene_info['scene_type'][0]
        self.scene_id = self.scene_info['scene_id'][0]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_type = self.data_provider.store_type
        self.kpi_static_data = self.common.get_kpi_static_data()

    def scene_score(self):
        if self.scene_type in Const.BAY_COUNTS:
            calculate_function = self.calculate_by_bays
        elif self.scene_type in Const.SCENE_TYPE_COUNTS:
            calculate_function = self.calculate_by_scene_types
        else:
            Log.warning("The scene_type {} has no definition".format(self.scene_type))
            return
        for i, product in self.scif.iterrows():
            product_fk = product['product_fk']
            facings = product['facings']
            pocs = calculate_function(product_fk)

    @staticmethod
    def calculate_by_scene_types(product_fk):
        return 1

    def calculate_by_bays(self, product_fk):
        relevant_match_product = self.match_product_in_scene[
            self.match_product_in_scene['product_fk'] == product_fk]
        return len(relevant_match_product['bay_number'].unique())

