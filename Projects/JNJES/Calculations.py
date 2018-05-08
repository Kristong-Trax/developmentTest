
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


from KPIUtils.GlobalProjects.JNJ.KPIGenerator import JNJGenerator
from KPIUtils.DB.Common import Common

__author__ = 'nissand'


class JNJESCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        jnj_generator = JNJGenerator(self.data_provider, self.output, common)
        jnj_generator.calculate_auto_assortment()
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('jnjes calculations')
#     Config.init()
#     project_name = 'jnjes'
#     data_provider = KEngineDataProvider(project_name)
#     session = ''
#     data_provider.load_session_data(session)
#     output = Output()
#     JNJESCalculations(data_provider, output).run_project_calculations()
