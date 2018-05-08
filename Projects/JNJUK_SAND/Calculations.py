from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
import pandas as pd

from KPIUtils.GlobalProjects.JNJ.KPIGenerator import JNJGenerator
from KPIUtils.DB.Common import Common

__author__ = 'nissand'


class JNJUKCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'JNJUK_SAND', 'Data', 'SurveyTemplate.xlsx')
        survey_template = pd.read_excel(TEMPLATE_PATH, sheetname='Sheet1')
        jnj_generator = JNJGenerator(self.data_provider, self.output, common)
        jnj_generator.secondary_placement_location_quality(survey_template)
        jnj_generator.secondary_placement_location_visibility_quality(survey_template)
        jnj_generator.calculate_auto_assortment()
        jnj_generator.promo_calc()
        jnj_generator.eye_hand_level_sos_calculation()
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('jnjuk-sand calculations')
#     Config.init()
#     project_name = 'jnjuk-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '9a96e442-12ec-4f11-9c20-5ce06bc6b1d7'
#     data_provider.load_session_data(session)
#     output = Output()
#     JNJUKCalculations(data_provider, output).run_project_calculations()
