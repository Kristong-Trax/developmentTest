from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from Trax.Algo.Calculations.Core.DataProvider import Data

__author__ = 'michaela'


class DIAGEONGSANDToolBox:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.commonV2)
        self.commonV2 = CommonV2(self.data_provider)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]

    def main_calculation(self):
        # Global assortment kpis - v2 for API use
        assortment_res_dict_v2 = self.diageo_generator.diageo_global_assortment_function_v2()
        self.commonV2.save_json_to_new_tables(assortment_res_dict_v2)

        # Global assortment kpis - v3 for NEW MOBILE REPORTS use
        assortment_res_dict_v3 = self.diageo_generator.diageo_global_assortment_function_v3()
        self.commonV2.save_json_to_new_tables(assortment_res_dict_v3)

        # Visible to Consumer %
        res_dict = self.diageo_generator.diageo_global_visible_percentage()
        self.commonV2.save_json_to_new_tables(res_dict)

        # Share of Shelf (SOS) %
        self.diageo_generator.activate_ootb_kpis(self.commonV2)

        # committing to the new tables
        self.commonV2.commit_results_data()

