import os
import pandas as pd
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from KPIUtils.GlobalProjects.JNJ.KPIGenerator_v2 import JNJGenerator
from KPIUtils_v2.DB.CommonV2 import Common
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


__author__ = 'nissand'


class JNJESCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        eye_level_data, exclusion_data = self._parse_templates_for_calculations()
        common = Common(self.data_provider)
        jnj_generator = JNJGenerator(self.data_provider, self.output, common, exclusion_data)
        jnj_generator.msl_availability()
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
#     LoggerInitializer.init('jnjes calculations')
#     Config.init()
#     project_name = 'jnjes'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = ['C002F40F-56C9-4025-BCBB-03F83D53402A',
#                 '6109F0FF-D22E-42B4-87B4-3A8CE83EC09B',
#                 'E7C6ED96-09BC-47B8-A827-D5E79D130473']
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         JNJESCalculations(data_provider, output).run_project_calculations()
