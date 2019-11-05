from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil import TemplateHandler
from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from Projects.DIAGEOCO.Data.Const import Const
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames, Consts
import pandas as pd

__author__ = 'huntery'


class DIAGEOCOToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.common_v2 = CommonV2(self.data_provider)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.store_id = self.data_provider.store_fk
        self.template_handler = TemplateHandler(self.data_provider.project_name)
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common, menu=True)

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """

        self.template_handler.update_templates()

        # SOS Out Of The Box kpis
        self.diageo_generator.activate_ootb_kpis(self.common_v2)

        # sos by scene type
        self.diageo_generator.sos_by_scene_type(self.common_v2)

        # Global assortment kpis
        assortment_res_dict = self.diageo_generator.diageo_global_assortment_function_v2()
        self.common_v2.save_json_to_new_tables(assortment_res_dict)

        # Global assortment kpis - v3 for NEW MOBILE REPORTS use
        assortment_res_dict_v3 = self.diageo_generator.diageo_global_assortment_function_v3()
        self.common_v2.save_json_to_new_tables(assortment_res_dict_v3)

        # Menu kpis
        menus_res_dict = self.diageo_generator.diageo_global_share_of_menu_cocktail_function()
        self.common_v2.save_json_to_new_tables(menus_res_dict)

        # Global Secondary Displays KPI
        res_json = self.diageo_generator.diageo_global_secondary_display_secondary_function()
        if res_json:
            self.common_v2.write_to_db_result(fk=res_json['fk'], numerator_id=res_json['numerator_id'],
                                              denominator_id=self.store_id,
                                              result=res_json['result'])

        # Global Brand Pouring KPI
        template_data = self.template_handler.download_template(DiageoKpiNames.BRAND_POURING)
        results_list = self.diageo_generator.diageo_global_brand_pouring_status_function(template_data)
        self.save_results_to_db(results_list)

        # Global Brand Blocking KPI
        template_data = self.template_handler.download_template(DiageoKpiNames.BRAND_BLOCKING)
        results_list = self.diageo_generator.diageo_global_block_together(DiageoKpiNames.BRAND_BLOCKING, template_data)
        self.save_results_to_db(results_list)

        # Global Relative Position KPI
        template_data = self.template_handler.download_template(DiageoKpiNames.RELATIVE_POSITION)
        results_list = self.diageo_generator.diageo_global_relative_position_function(template_data)
        self.save_results_to_db(results_list)

        # Global TOUCH_POINT KPI
        templates_data = pd.read_excel(Const.TEMPLATE_PATH, Const.TOUCH_POINT_SHEET_NAME,
                                       header=Const.TOUCH_POINT_HEADER_ROW)
        template = templates_data.fillna(method='ffill').set_index(templates_data.keys()[0])
        results_list = self.diageo_generator.diageo_global_touch_point_function(template=template, old_tables=True,
                                                                                new_tables=False,
                                                                                store_attribute=Consts.ADDITIONAL_ATTR_2)
        self.save_results_to_db(results_list)

        # committing to new tables
        self.common_v2.commit_results_data()
        # committing to the old tables
        self.common.commit_results_data()

    def save_results_to_db(self, results_list):
        if results_list:
            for result in results_list:
                if result is not None:
                    self.common_v2.write_to_db_result(**result)
