from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil import TemplateHandler


class DIAGEOUKToolBox:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.output = output
        self.common = Common(self.data_provider)
        self.commonV2 = CommonV2(self.data_provider)
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common, menu=True)
        self.template_handler = TemplateHandler(self.project_name)

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        log_runtime('Updating templates')
        self.template_handler.update_templates()

        # SOS Out Of The Box kpis
        self.diageo_generator.activate_ootb_kpis(self.commonV2)

        # sos by scene type
        self.diageo_generator.sos_by_scene_type(self.commonV2)

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

        # Global Equipment score kpis
        equipment_score_scenes = self.get_equipment_score_relevant_scenes()
        res_dict = self.diageo_generator.diageo_global_equipment_score(save_scene_level=False,
                                                                       scene_list=equipment_score_scenes)
        self.commonV2.save_json_to_new_tables(res_dict)

        # Global Relative Position function
        template_data = self.template_handler.download_template(DiageoKpiNames.RELATIVE_POSITION)
        res_dict = self.diageo_generator.diageo_global_relative_position_function(
            template_data, location_type='template_name')
        self.commonV2.save_json_to_new_tables(res_dict)

        # Global Secondary Displays function
        res_json = self.diageo_generator.diageo_global_secondary_display_secondary_function()
        if res_json:
            self.commonV2.write_to_db_result(fk=res_json['fk'], numerator_id=1, denominator_id=self.store_id,
                                             result=res_json['result'])

        # Global Visible to Consumer function
        sku_list = filter(None, self.scif[self.scif['product_type'] == 'SKU'].product_ean_code.tolist())
        res_dict = self.diageo_generator.diageo_global_visible_percentage(sku_list)
        self.commonV2.save_json_to_new_tables(res_dict)

        # committing to new tables
        self.commonV2.commit_results_data()
        # committing to the old tables
        self.common.commit_results_data()

    def get_equipment_score_relevant_scenes(self):
        scenes = []
        if not self.diageo_generator.scif.empty:
            scenes = self.diageo_generator.scif[self.diageo_generator.scif['template_name'] ==
                                                'ON - DRAUGHT TAPS']['scene_fk'].unique().tolist()
        return scenes
