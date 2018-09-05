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
        empties = self.all_products[self.all_products['product_type'] == 'Empty']['product_fk'].unique().tolist()
        self.match_product_in_scene = self.match_product_in_scene[
            ~(self.match_product_in_scene['product_fk'].isin(empties))]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.template_group = self.templates['template_group'].iloc[0]
        self.scene_id = self.scene_info['scene_fk'][0]
        self.kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.POC)
        self.poc_number = 1

    def scene_score(self):
        if self.template_group in Const.DICT_WITH_TYPES.keys():
            for poc in self.match_product_in_scene[Const.DICT_WITH_TYPES[self.template_group]].unique().tolist():
                relevant_match_products = self.match_product_in_scene[self.match_product_in_scene[
                    Const.DICT_WITH_TYPES[self.template_group]] == poc]
                self.count_products(relevant_match_products)
                self.poc_number += 1
        else:
            Log.warning("scene_type {} is not supported for points of contact".format(self.template_group))

    def count_products(self, relevant_match_products):
        for product_fk in relevant_match_products['product_fk'].unique().tolist():
            facings = len(relevant_match_products[relevant_match_products['product_fk'] == product_fk])
            self.common.write_to_db_result(fk=self.kpi_fk, numerator_id=product_fk, numerator_result=facings,
                                           result=self.poc_number, by_scene=True)
