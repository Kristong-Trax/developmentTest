
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator
import os


__author__ = 'Shani'


class SANOFIRUSandCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIRU_SAND',
                                     'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Logging.Logger import Log
# if __name__ == '__main__':
#     LoggerInitializer.init('sanofiru-sand calculations')
#     Config.init()
#     project_name = 'sanofiru-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'C18C862E-ECCA-4181-98CF-DB595FBFE1FD'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIRUCalculations(data_provider, output).run_project_calculations()
