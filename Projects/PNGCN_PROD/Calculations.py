
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Logging.Logger import Log

from Projects.PNGCN_PROD.KPIToolBox import PNGToolBox, log_runtime

__author__ = 'ortalk'


class PngCNEmptyCalculations(BaseCalculationsScript):

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):
        self.timer.start()
        tool_box = PNGToolBox(self.data_provider, self.output)
        if not tool_box.check_validation_of_session():
            Log.info('Session has no relevant scenes')
        elif not tool_box.empty_spaces.keys():
            Log.info('None of the categories is relevant for this visit')
        else:
            tool_box.main_calculation()
        tool_box.commit_results_data()
        self.timer.stop('PngCNEmptyCalculations.run_project_calculations')

if __name__ == '__main__':
    LoggerInitializer.init('Png-cn calculations')
    Config.init()
    project_name = 'pngcn-prod'
    data_provider = KEngineDataProvider(project_name)
    session = 'cb2cc33d-de43-4c35-a25b-ce538730037e'
    data_provider.load_session_data(session)
    output = Output()
    PngCNEmptyCalculations(data_provider, output).run_project_calculations()
