
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
import os
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'idanr'


class SANOFIOCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIJO', 'Data',
                                     'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, template_path).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
#
# if __name__ == '__main__':
#     LoggerInitializer.init('sanofijo calculations')
#     Config.init()
#     project_name = 'sanofijo'
#     data_provider = KEngineDataProvider(project_name)
#     session = ''
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIUZCalculations(data_provider, output).run_project_calculations()
