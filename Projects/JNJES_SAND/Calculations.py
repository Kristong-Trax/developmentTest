import os
import pandas as pd
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from KPIUtils.GlobalProjects.JNJ.KPIGenerator_v2 import JNJGenerator
from KPIUtils_v2.DB.CommonV2 import Common

__author__ = 'nissand'


class JNJESSANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        eye_level_data, exclusion_data = self._parse_templates_for_calculations()
        common = Common(self.data_provider)
        jnj_generator = JNJGenerator(self.data_provider, self.output, common, exclusion_data)
        jnj_generator.linear_sos_out_of_store_discovery_report()
        jnj_generator.share_of_shelf_manufacturer_out_of_sub_category()
        jnj_generator.calculate_auto_assortment()
        jnj_generator.eye_hand_level_sos_calculation(eye_level_data)
        jnj_generator.promo_calc(sales_reps_date='2019-06-30')
        common.commit_results_data()
        self.timer.stop('KPIGenerator.run_project_calculations')

    @staticmethod
    def _parse_templates_for_calculations():
        """ This method parse the local relevant template for the global code calculation"""
        data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data')
        eye_hand_lvl_template_path = os.path.join(data_path, 'eye_level_jnjes.xlsx')
        exclusive_template_path = os.path.join(data_path, 'KPI Exclusions Template.xlsx')
        eye_hand_lvl_template = pd.read_excel(eye_hand_lvl_template_path)
        exclusion_template = pd.read_excel(exclusive_template_path)
        return eye_hand_lvl_template, exclusion_template


# if __name__ == '__main__':
#     LoggerInitializer.init('jnjes-sand calculations')
#     Config.init()
#     project_name = 'jnjes-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'C4035046-E901-48FC-9858-901A6B43FF95'
#     data_provider.load_session_data(session)
#     output = Output()
#     JNJESSANDCalculations(data_provider, output).run_project_calculations()
