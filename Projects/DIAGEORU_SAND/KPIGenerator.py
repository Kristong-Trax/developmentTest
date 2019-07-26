
from Trax.Utils.Logging.Logger import Log
from Projects.DIAGEORU_SAND.Utils.KPIToolBox import DIAGEORUToolBox, log_runtime


class DIAGEORUGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.tool_box = DIAGEORUToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
