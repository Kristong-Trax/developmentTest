
from Trax.Utils.Logging.Logger import Log
from Projects.CBCDAIRYIL.Utils.KPIToolBox import CBCDAIRYILToolBox
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'idanr'


class Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.tool_box = CBCDAIRYILToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function - where the magic happens.
        It calculates the score for every KPI and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
            return
        self.tool_box.main_calculation()
        self.tool_box.common.commit_results_data()
        self.tool_box.old_common.commit_results_data()

