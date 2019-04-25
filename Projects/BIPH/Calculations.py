
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class BIPHCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        if str(self.data_provider.visit_date) >= '2018-02-01' and str(self.data_provider.visit_date) <= '2018-02-28':
            TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'BIPH',
                                         'Data', 'Template_old.xlsx')
        else:
            TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'BIPH', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofiph calculations')
#     Config.init()
#     project_name = 'biph'
#     data_provider = KEngineDataProvider(project_name)
#     session = '714cf05f-6fca-47e4-b598-eb8c95107b7f'
#     data_provider.load_session_data(session)
#     output = Output()
#     BIPHCalculations(data_provider, output).run_project_calculations()
