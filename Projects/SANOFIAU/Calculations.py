import os

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator

__author__ = 'Shani'

class SANOFIAUCalculations(BaseCalculationsScript):
    """SANOFIAUCalculations class inherits the BaseCalculationsScript"""
    def run_project_calculations(self):
        """
        run_project_calculations function is the entry point function to run the calculation
        """
        self.timer.start()
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIAU', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, template_path).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofiau calculations')
#     Config.init()
#     project_name = 'sanofiau'
#     data_provider = KEngineDataProvider(project_name)
#     session = '3D081D8D-D6EF-4944-8926-9BC07D2DB22C'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIAUCalculations(data_provider, output).run_project_calculations()
