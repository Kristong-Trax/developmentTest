from Trax.Algo.Calculations.Core.DataProvider import Data
import os
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2


class DIAGEOIEToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.output = output
        self.common = Common(self.data_provider)
        self.commonV2 = CommonV2(self.data_provider)
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common, menu=True)

    def main_calculation(self):
        """
        This function calculates the KPI results.
         """
        # SOS Out Of The Box kpis
        self.diageo_generator.activate_ootb_kpis(self.commonV2)

        # Global assortment kpis
        assortment_res_dict = self.diageo_generator.diageo_global_assortment_function_v2()
        self.commonV2.save_json_to_new_tables(assortment_res_dict)

        # Global assortment kpis - v3 for NEW MOBILE REPORTS use
        assortment_res_dict_v3 = self.diageo_generator.diageo_global_assortment_function_v3()
        self.commonV2.save_json_to_new_tables(assortment_res_dict_v3)

        # Global Menu kpis
        menus_res_dict = self.diageo_generator.diageo_global_share_of_menu_cocktail_function(
            cocktail_product_level=True)
        self.commonV2.save_json_to_new_tables(menus_res_dict)

        # Global Tap Brand Score
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                     'Data', 'Brand Score.xlsx')
        res_dict = self.diageo_generator.diageo_global_tap_brand_score_function(template_path, save_to_tables=False,
                                                                                calculate_components=True)
        self.commonV2.save_json_to_new_tables(res_dict)

        # Global Visible to Consumer function
        sku_list = filter(None, self.scif[self.scif['product_type'] == 'SKU'].product_ean_code.tolist())
        res_dict = self.diageo_generator.diageo_global_visible_percentage(sku_list)
        if res_dict:
            self.commonV2.save_json_to_new_tables(res_dict)

        # committing to new tables
        self.commonV2.commit_results_data()
        # committing to the old tables
        self.common.commit_results_data()
