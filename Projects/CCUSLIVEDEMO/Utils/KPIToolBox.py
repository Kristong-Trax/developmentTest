__author__ = 'limor'

from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils.DB.Common import Common
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator


class CCUSLiveDemoToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.common_v2 = CommonV2(self.data_provider)
        self.common = Common(self.data_provider)
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common)

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        # Global assortment kpis
        assortment_res_dict = self.diageo_generator.diageo_global_assortment_function_v2()
        self.common_v2.save_json_to_new_tables(assortment_res_dict)
