
from Projects.PEPSIUSV2.Utils.KPIToolBox import PepsiUSV2ToolBox
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Trax.Utils.Logging.Logger import Log

__author__ = 'idanr'


class PepsiUSV2Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.tool_box = PepsiUSV2ToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session.')
        else:
            self.tool_box.main_calculation()
