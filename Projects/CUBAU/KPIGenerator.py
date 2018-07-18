
from Trax.Utils.Logging.Logger import Log
from Projects.CUBAU.Utils.KPIToolBox import CUBAUCUBAUToolBox, log_runtime
from Projects.CUBAU.Utils.ToolsSummary import Summary
from Trax.Algo.Calculations.Core.DataProvider import Data

__author__ = 'Shani'


class CUBAUGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = CUBAUCUBAUToolBox(self.data_provider, self.output)
        self.tools_summary = Summary(self.data_provider, self.output)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
        self.tools_summary.main_calculation()
        # self.tool_box.common.commit_results_data_to_new_tables()
