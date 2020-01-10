
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

import os
import pandas as pd

from KPIUtils.GlobalProjects.JNJ.KPIGenerator import JNJGenerator
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'nissand'


class JNJITCalculations(BaseCalculationsScript):

    @log_runtime(description="Total Calculation")
    def run_project_calculations(self):
        self.timer.start()
        eye_hand_lvl_template, survey_template = self._parse_templates_for_calculations()
        common = Common(self.data_provider)
        jnj_generator = JNJGenerator(self.data_provider, self.output, common)
        jnj_generator.secondary_placement_location_quality(survey_template)
        jnj_generator.secondary_placement_location_visibility_quality(survey_template)
        jnj_generator.calculate_auto_assortment()
        jnj_generator.promo_calc(sales_reps_date='2018-09-30')
        jnj_generator.eye_hand_level_sos_calculation(eye_hand_lvl_template)
        jnj_generator.linear_sos_out_of_store_discovery_report()
        common.commit_results_data()
        self.timer.stop('KPIGenerator.run_project_calculations')

    @staticmethod
    def _parse_templates_for_calculations():
        """ This method parse the local relevant template for the global code calculation"""
        data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data')
        eye_hand_lvl_template_path = os.path.join(data_path, 'eye_level_jnjit.xlsx')
        survey_template_path = os.path.join(data_path, 'SurveyTemplate.xlsx')
        eye_hand_lvl_template = pd.read_excel(eye_hand_lvl_template_path)
        survey_template = pd.read_excel(survey_template_path, sheetname='Sheet1')
        return eye_hand_lvl_template, survey_template


# if __name__ == '__main__':
#     LoggerInitializer.init('jnjit calculations')
#     Config.init()
#     project_name = 'jnjit'
#     data_provider = KEngineDataProvider(project_name)
#     session = '1ADB8F9A-443A-42AB-9AD2-9F91FB29E5F2'
#     data_provider.load_session_data(session)
#     output = Output()
#     JNJITCalculations(data_provider, output).run_project_calculations()
