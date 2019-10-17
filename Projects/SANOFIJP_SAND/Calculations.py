
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Logging.Logger import Log
from KPIUtils.GlobalProjects.SANOFI_2.Utils.KPIToolBox import log_runtime
from Projects.SANOFIJP.Utils.KPIToolBox import SanofiJPToolBox
import os
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'nidhinb'


class SANOFIJPCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        # Global KPI calcs -- the commit is done with this!
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIJP', 'Data', 'Template.xlsx')
        sanofijp_main = SanofiJPGenerator(self.data_provider, self.output, TEMPLATE_PATH)
        sanofijp_main.main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


class SanofiJPGenerator(SANOFIGenerator, object):
    def __init__(self, data_provider, output, template_path):
        super(SanofiJPGenerator, self).__init__(data_provider, output, template_path)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.write_time_frame()
        self.tool_box.calculate_perfect_store()
        # For Custom KPI
        SanofiJPToolBox(self.data_provider, self.output, self.tool_box.common).main_calculation()
        self.tool_box.commit_results_data()

# if __name__ == '__main__':
#     LoggerInitializer.init('sanofijp calculations')
#     Config.init()
#     project_name = 'sanofijp'
#     data_provider = KEngineDataProvider(project_name)
#     session = '31eb50cd-e799-4798-a27f-dc5ea557ccc7'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIJPCalculations(data_provider, output).run_project_calculations()
