
from Trax.Utils.Logging.Logger import Log

from Projects.CUBAU_SAND.Utils.KPIToolBox import CUBAUToolBox, log_runtime

__author__ = 'Shani'


class CUBAUGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = CUBAUToolBox(self.data_provider, self.output)


    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
        self.tool_box.common.commit_results_data_to_new_tables()
