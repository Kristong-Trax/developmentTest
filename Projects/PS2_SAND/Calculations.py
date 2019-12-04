
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from KPIUtils.GlobalProjects.SANOFI_2.KPIGeneratorTestTemporary import SANOFIGeneratorTest
import os


__author__ = 'Shani'


class PS2SandCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIEG',
                                     'Data', 'Template.xlsx')
        SANOFIGeneratorTest(self.data_provider, self.output, template_path).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Logging.Logger import Log
# if __name__ == '__main__':
#     LoggerInitializer.init('ps2-sand calculations')
#     Config.init()
#     project_name = 'ps2-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'b27afe27-44f2-4148-8029-921ff8ca69fb'
#     data_provider.load_session_data(session)
#     output = Output()
#     PS2SandCalculations(data_provider, output).run_project_calculations()
