
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class BISEASKCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        if str(self.data_provider.visit_date)>='2018-02-01' and str(self.data_provider.visit_date)<='2018-02-28':
            TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'BISEASK', 'Data', 'Template_Old.xlsx')
        else:
            TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'BISEASK',
                                         'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofiseask calculations')
#     Config.init()
#     project_name = 'biseask'
#     data_provider = KEngineDataProvider(project_name)
#     session = '77408F47-4EE3-4A68-B02B-00E0892BC47C'
#     data_provider.load_session_data(session)
#     output = Output()
#     BISEASKCalculations(data_provider, output).run_project_calculations()
