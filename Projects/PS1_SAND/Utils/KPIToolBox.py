from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil import TemplateHandler


class PS1SandToolBox:

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
         # SOS Out Of The Box kpis
        self.diageo_generator.activate_ootb_kpis(self.commonV2)

        # sos by scene type
        self.diageo_generator.sos_by_scene_type(self.commonV2)

        # Global assortment kpis
        assortment_res = self.diageo_generator.diageo_global_grouping_assortment_calculation()
        self.commonV2.save_json_to_new_tables(assortment_res)

        # Global Menu kpis
        menus_res = self.diageo_generator.diageo_global_new_share_of_menu_function()
        self.commonV2.save_json_to_new_tables(menus_res)

        # Global Secondary Displays function
        res_json = self.diageo_generator.diageo_global_secondary_display_secondary_function()
        if res_json:
            self.commonV2.write_to_db_result(fk=res_json['fk'], numerator_id=1, denominator_id=self.store_id,
                                             result=res_json['result'])

        # committing to new tables
        self.commonV2.commit_results_data()
        # committing to the old tables
        self.common.commit_results_data()

