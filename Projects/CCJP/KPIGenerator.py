from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.CCJP.Utils.KPIToolBox import ToolBox
__author__ = 'satya'


class Generator:

    def __init__(self, data_provider, output, set_up_file):
        self.data_provider = data_provider
        self.output = output
        self.set_up_file = set_up_file
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.tool_box = ToolBox(self.data_provider, self.output, set_up_file)


    STORE = "Store"
    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
#        self.tool_box.commit_results()




