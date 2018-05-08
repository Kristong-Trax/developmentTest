from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output
# from Trax.Algo.Calculations.Core.Vanilla import SessionVanillaCalculations
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Logging.Logger import Log

from Projects.INTEG15.EmptySpacesKpi import INTEG15EmptySpaceKpiGenerator

__author__ = 'ortalk'


class INTEG15PngCNEmptyCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Log.info('INTEG15 is running')
        INTEG15EmptySpaceKpiGenerator(self.data_provider, self.output, self.data_provider.project_name).main_function()
        self.timer.stop('PngCNEmptyCalculations.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('Png-cn calculations')
#     Config.init()
#     # project_name = 'pngcn-prod'
#     project_name = 'integ15'
#     data_provider = ACEDataProvider(project_name)
#     session = 'aa0ea0fc-4ed8-4852-a1ee-af513723dd89'
#     # for session in sessions:
#     data_provider.load_session_data(session)
#     output = Output()
#     SessionVanillaCalculations(data_provider, output, project_name).run_project_calculations()
#     INTEG15PngCNEmptyCalculations(data_provider, output).run_project_calculations()
#     data_provider.export_session_data(output)