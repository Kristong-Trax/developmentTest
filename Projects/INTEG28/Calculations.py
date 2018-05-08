
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log


from Projects.INTEG28.KPIToolBox import INTEG28PNGToolBox, log_runtime

__author__ = 'ortalk'


class INTEG28PngCNEmptyCalculations(BaseCalculationsScript):

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):
        self.timer.start()
        tool_box = INTEG28PNGToolBox(self.data_provider, self.output)
        if not tool_box.check_validation_of_session():
            Log.info('Session has no relevant scenes')
        elif not tool_box.empty_spaces.keys():
            Log.info('None of the categories is relevant for this visit')
        else:
            tool_box.main_calculation()
        tool_box.commit_results_data()
        self.timer.stop('INTEG28PngCNEmptyCalculations.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('Png-cn calculations')
#     Config.init()
#     project_name = 'integ28'
#     data_provider = KEngineDataProvider(project_name)
#     session = '5fc262aa-de2d-4721-ada5-2534921214ba'
#     data_provider.load_session_data(session)
#     output = Output()
#     INTEG28PngCNEmptyCalculations(data_provider, output).run_project_calculations()
