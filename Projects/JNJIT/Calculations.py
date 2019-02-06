
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
        survey_template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'JNJIT', 'Data',
                                     'SurveyTemplate.xlsx')
        eye_hand_lvl_template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'JNJIT', 'Data',
                                     'eye_level_jnjuk.xlsx')
        survey_template = pd.read_excel(survey_template_path, sheetname='Sheet1')
        eye_hand_lvl_template = pd.read_excel(eye_hand_lvl_template_path)
        common = Common(self.data_provider)
        jnj_generator = JNJGenerator(self.data_provider, self.output, common)
        jnj_generator.secondary_placement_location_quality(survey_template)
        jnj_generator.secondary_placement_location_visibility_quality(survey_template)
        jnj_generator.calculate_auto_assortment()
        jnj_generator.promo_calc(sales_reps_date='2018-09-30')
        jnj_generator.eye_hand_level_sos_calculation(eye_hand_lvl_template)
        common.commit_results_data()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('jnjit calculations')
#     Config.init()
#     project_name = 'jnjit'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'EE25A944-7389-482A-95A3-248A077A12D4'
#     data_provider.load_session_data(session)
#     output = Output()
#     JNJITCalculations(data_provider, output).run_project_calculations()
