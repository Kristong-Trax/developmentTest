
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Logging.Logger import Log

from Projects.PNGCN_SAND.KPIToolBox import PNGCN_SANDPNGToolBox, log_runtime

__author__ = 'ortalk'


class PNGCN_SANDPngCNEmptyCalculations(BaseCalculationsScript):

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):
        self.timer.start()
        tool_box = PNGCN_SANDPNGToolBox(self.data_provider, self.output)
        tool_box.calculate_share_of_display()
        if not tool_box.check_validation_of_session():
            Log.info('Session has no relevant scenes')
        elif not tool_box.empty_spaces.keys():
            Log.info('None of the categories is relevant for this visit')
        else:
            tool_box.main_calculation()
        tool_box.commit_results_data()
        self.timer.stop('PNGCN_SANDPngCNEmptyCalculations.run_project_calculations')

if __name__ == '__main__':
    LoggerInitializer.init('Png-cn calculations')
    Config.init()
    project_name = 'pngcn-sand'
    data_provider = KEngineDataProvider(project_name)
    session = 'e0226bdf-2bc5-4c3b-9c50-6b7cd04fcb4b'
    data_provider.load_session_data(session)
    output = Output()
    PNGCN_SANDPngCNEmptyCalculations(data_provider, output).run_project_calculations()
