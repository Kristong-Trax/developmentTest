from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
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


# if __name__ == '__main__':
#     LoggerInitializer.init('jnjuk-sand calculations')
#     Config.init()
#     project_name = 'jnjuk-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = "cff3c170-1ba9-4ce4-bb24-f784827d953d"
#     sessions = ["788b26b7-25a2-4140-ac5d-61b67a3852c4", "7c8d5ef2-3881-4cef-a7bc-8c2512c3ed74", "62e4df75-2345-414e-aac5-bfeab3b51634",
#                 "d5108b7f-fbeb-4f21-bfeb-4998ede09e4b", "30c1b027-a44a-4060-b19b-bf806ff19075"]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         JNJUKCalculations(data_provider, output).run_project_calculations()
