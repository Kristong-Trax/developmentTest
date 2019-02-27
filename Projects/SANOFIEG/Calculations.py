from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator


__author__ = 'Idan'


class SANOFIEGCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIEG', 'Data', 'Template.xlsx')
        TEMPLATE_PATH2 = template_path.replace("/Template.xlsx", "/template_jan.xlsx")
        SANOFIGenerator(self.data_provider, self.output, template_path, template2=TEMPLATE_PATH2).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofieg calculations')
#     Config.init()
#     project_name = 'sanofieg'
#     data_provider = KEngineDataProvider(project_name)
#     session = '2DC2AE20-D576-408D-8A78-18CB16445254'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIEGCalculations(data_provider, output).run_project_calculations()
