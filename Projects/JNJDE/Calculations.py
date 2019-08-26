
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
import os
import pandas as pd

from KPIUtils.GlobalProjects.JNJ.KPIGenerator import JNJGenerator
from KPIUtils_v2.DB.CommonV2 import Common

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'israels'


class JNJDECalculations(BaseCalculationsScript):

    @log_runtime(description="Total Calculation")
    def run_project_calculations(self):
        self.timer.start()
        eye_hand_lvl_template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                                  'JNJDE', 'Data', 'eye_level_jnjde.xlsx')
        eye_hand_lvl_template = pd.read_excel(eye_hand_lvl_template_path)
        common = Common(self.data_provider)
        jnj_generator = JNJGenerator(self.data_provider, self.output, common)

        # KPI 2 - Share of Shelf
        jnj_generator.sos_vs_target_calculation()
        # KPI 3 - OOS
        jnj_generator.calculate_auto_assortment()
        # KPI 4 - Share of shelf - Hand & Eye
        # KPI 12 - New H&E KPI
        # KPI 13 - Share of shelf - Hand & Eye (Sub-Category)
        jnj_generator.eye_hand_level_sos_calculation(eye_hand_lvl_template)
        jnj_generator.eye_hand_level_sos_calculation_de(eye_hand_lvl_template)
        # KPI 5 IR - Activation compliance vs plans
        jnj_generator.promo_calc(sales_reps_date='2018-09-30', calc_brand_level=True)
        # KPI 9 - MSL
        jnj_generator.assortment_calculation()
        # KPI 10 - New Display compliance
        jnj_generator.display_compliance_calculation()
        # Competitor KPI's
        jnj_generator.sos_brand_out_of_sub_category()
        jnj_generator.competitor_eye_hand_level_sos(eye_hand_lvl_template)
        common.commit_results_data()
        self.timer.stop('KPIGenerator.run_project_calculations')
