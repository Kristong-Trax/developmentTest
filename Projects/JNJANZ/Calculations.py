import os
import pandas as pd
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from KPIUtils.GlobalProjects.JNJ.KPIGenerator_v2 import JNJGenerator
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JNJANZ.Utils.LocalAssortKPIToolBox import JNJToolBox



__author__ = 'ilays'


class JNJANZCalculations(BaseCalculationsScript):

    @log_runtime(description="Total Calculation")
    def run_project_calculations(self):
        self.timer.start()
        eye_level_data, exclusion_data = self._parse_templates_for_calculations()
        common = Common(self.data_provider)
        try:
            jnj_generator = JNJGenerator(self.data_provider, self.output, common, exclusion_data)
            # Local MSL Assortment KPI with hierarchy
            self.calculate_local_assortment(self.data_provider, self.output, common, exclusion_data)

            # Mobile KPIs with hierarchy
            jnj_generator.calculate_auto_assortment(in_balde=False, hierarchy=True,
                                                    just_primary=False, filter_sub_categories=False)
            jnj_generator.eye_hand_level_sos_calculation(eye_level_data, hierarchy=True)
            jnj_generator.promo_calc(hierarchy=True)
            jnj_generator.lsos_with_hierarchy()

            # API global KPIs
            jnj_generator.linear_sos_out_of_store_discovery_report()
            jnj_generator.calculate_auto_assortment(in_balde=False)
            jnj_generator.promo_calc()
            jnj_generator.eye_hand_level_sos_calculation(eye_level_data)

        except Exception as e:
            print ("Error :{}".format(e))

        common.commit_results_data()
        self.timer.stop('KPIGenerator.run_project_calculations')

    def calculate_local_assortment(self, data_provider, output, common, exclusion_data):
        # LOCAL MSL / OSS Assortment KPIs
        try:
            local_assort_toolbox = JNJToolBox(data_provider, output, common,  exclusion_data)
            local_assort_toolbox.main_calculation()
        except Exception as e:
            print ("Error: {}".format(e))

    @staticmethod
    def _parse_templates_for_calculations():
        """ This method parse the local relevant template for the global code calculation"""
        data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data')
        eye_hand_lvl_template_path = os.path.join(data_path, 'eye_level_jnjanz.xlsx')
        exclusive_template_path = os.path.join(data_path, 'KPI Exclusions Template.xlsx')
        eye_hand_lvl_template = pd.read_excel(eye_hand_lvl_template_path)
        exclusion_template = pd.read_excel(exclusive_template_path)
        return eye_hand_lvl_template, exclusion_template


# if __name__ == '__main__':
#     LoggerInitializer.init('jnjanz calculations')
#     Config.init()
#     project_name = 'jnjanz'
#     data_provider = KEngineDataProvider(project_name)
#     session = '443782B5-44DB-47E8-B40B-E4C498DA5A4A'
#     data_provider.load_session_data(session)
#     output = Output()
#     JNJANZCalculations(data_provider, output).run_project_calculations()
