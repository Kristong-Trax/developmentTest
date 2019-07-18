import os
import pandas as pd
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


from KPIUtils.GlobalProjects.JNJ.KPIGenerator import JNJGenerator
from KPIUtils_v2.DB.CommonV2 import Common


__author__ = 'nissand'


class JNJESCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        eye_hand_lvl_template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'JNJES',
                                                  'Data',
                                                  'eye_level_jnjes.xlsx')
        eye_hand_lvl_template = pd.read_excel(eye_hand_lvl_template_path)
        common = Common(self.data_provider)
        jnj_generator = JNJGenerator(self.data_provider, self.output, common)
        jnj_generator.calculate_auto_assortment()
        jnj_generator.eye_hand_level_sos_calculation(eye_hand_lvl_template)
        jnj_generator.promo_calc(sales_reps_date='2019-06-30')
        common.commit_results_data()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('jnjes calculations')
#     Config.init()
#     project_name = 'jnjes'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'F670CB0D-769B-44F2-89C9-96A16F4FF25E'
#     data_provider.load_session_data(session)
#     output = Output()
#     JNJESCalculations(data_provider, output).run_project_calculations()
