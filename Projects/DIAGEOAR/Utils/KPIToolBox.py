from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil import TemplateHandler
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames


class DIAGEOARDIAGEOARToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.common = Common(self.data_provider)
        self.commonV2 = CommonV2(self.data_provider)
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common)
        self.template_handler = TemplateHandler(self.project_name)

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        log_runtime('Updating templates')(self.template_handler.update_templates)()

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

        # Global Relative Position function
        template_data = self.template_handler.download_template(DiageoKpiNames.RELATIVE_POSITION)
        res_dict = self.diageo_generator.diageo_global_relative_position_function(
            template_data, location_type='template_name')
        self.commonV2.save_json_to_new_tables(res_dict)

        # committing to new tables
        self.commonV2.commit_results_data()
        # committing to the old tables
        self.common.commit_results_data()
