from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
import pandas as pd

from KPIUtils.GlobalProjects.JNJ.KPIGenerator_v2 import JNJGenerator
from KPIUtils_v2.DB.CommonV2 import Common

__author__ = 'nissand'


class JNJUKCalculations(BaseCalculationsScript):
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
        jnj_generator.calculate_auto_assortment(in_balde=False)
        jnj_generator.promo_calc_recovery()
        jnj_generator.eye_hand_level_sos_calculation(eye_hand_lvl_template)
        jnj_generator.general_assortment_calculation()
        jnj_generator.osa_calculation()
        common.commit_results_data()
        jnj_generator.tool_box.commit_osa_queries()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('jnjuk-sand calculations')
    Config.init()
    project_name = 'jnjuk-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = ["30c1b027-a44a-4060-b19b-bf806ff19075"]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        JNJUKCalculations(data_provider, output).run_project_calculations()
