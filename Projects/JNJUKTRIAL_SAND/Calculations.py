
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

import os
import pandas as pd

from KPIUtils.GlobalProjects.JNJ.KPIGenerator_v2 import JNJGenerator
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'nissand'


class JNJUKTRIAL_SANDCalculations(BaseCalculationsScript):

    @log_runtime(description="Total Calculation")
    def run_project_calculations(self):
        self.timer.start()
        data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data')
        survey_template_path = os.path.join(data_path, 'SurveyTemplate.xlsx')
        eye_hand_lvl_template_path = os.path.join(data_path, 'eye_level_jnjuk.xlsx')
        exclusive_template_path = os.path.join(data_path, 'KPI Exclusions Template.xlsx')

        survey_template = pd.read_excel(survey_template_path, sheetname='Sheet1')
        eye_hand_lvl_template = pd.read_excel(eye_hand_lvl_template_path)
        exclusive_template = pd.read_excel(exclusive_template_path)
        common = Common(self.data_provider)
        jnj_generator = JNJGenerator(self.data_provider, self.output, common, exclusive_template)
        jnj_generator.linear_sos_out_of_store_discovery_report()
        jnj_generator.secondary_placement_location_quality(survey_template)
        jnj_generator.secondary_placement_location_visibility_quality(survey_template)
        jnj_generator.share_of_shelf_manufacturer_out_of_sub_category()
        jnj_generator.calculate_auto_assortment(in_balde=True)
        jnj_generator.store_auto_assortment_stateless(3, in_blade=True, at_least_visits=2)
        jnj_generator.promo_calc(sales_reps_date='2018-05-31')
        jnj_generator.eye_hand_level_sos_calculation(eye_hand_lvl_template)
        common.commit_results_data()
        self.timer.stop('KPIGenerator.run_project_calculations')

