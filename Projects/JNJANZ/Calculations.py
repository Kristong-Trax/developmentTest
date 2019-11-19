import os
import pandas as pd
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from KPIUtils.GlobalProjects.JNJ.KPIGenerator_v2 import JNJGenerator
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


__author__ = 'ilays'


class JNJANZCalculations(BaseCalculationsScript):

    @log_runtime(description="Total Calculation")
    def run_project_calculations(self):
        self.timer.start()
        eye_level_data, exclusion_data, survey_data = self._parse_templates_for_calculations()
        common = Common(self.data_provider)
        jnj_generator = JNJGenerator(self.data_provider, self.output, common, exclusion_data)
        # jnj_generator.calculate_auto_assortment(in_balde=False, only_oos=True,
        #                                         just_primary=False, filter_sub_categories=False)
        jnj_generator.eye_hand_level_sos_calculation(eye_level_data, hierarchy=True)
        jnj_generator.promo_calc_recovery()
        # jnj_generator.linear_sos_out_of_store_discovery_report()
        # jnj_generator.share_of_shelf_manufacturer_out_of_sub_category()
        common.commit_results_data()
        jnj_generator.tool_box.commit_osa_queries()
        self.timer.stop('KPIGenerator.run_project_calculations')

    @staticmethod
    def _parse_templates_for_calculations():
        """ This method parse the local relevant template for the global code calculation"""
        data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data')
        eye_hand_lvl_template_path = os.path.join(data_path, 'eye_level_jnjanz.xlsx')
        exclusive_template_path = os.path.join(data_path, 'KPI Exclusions Template.xlsx')
        survey_template_path = os.path.join(data_path, 'SurveyTemplate.xlsx')
        eye_hand_lvl_template = pd.read_excel(eye_hand_lvl_template_path)
        exclusion_template = pd.read_excel(exclusive_template_path)
        survey_template = pd.read_excel(survey_template_path, sheetname='Sheet1')
        return eye_hand_lvl_template, exclusion_template, survey_template


# if __name__ == '__main__':
#     LoggerInitializer.init('jnjanz calculations')
#     Config.init()
#     project_name = 'jnjanz'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'b3ff7bc7-cbe1-46fe-ada7-712f0f9f070d'
#     # session = 'f850397b-6b79-47e9-897b-9edb2632efda' # 60 sec for promocalc
#     data_provider.load_session_data(session)
#     output = Output()
#     JNJANZCalculations(data_provider, output).run_project_calculations()
