
from Trax.Utils.Logging.Logger import Log

from Projects.RBUS.Utils.KPIToolBox import RBUSRBUSToolBox, log_runtime

__author__ = 'yoava'


class RBUSRBUSGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = RBUSRBUSToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        else:
            set_names = ['Placement count', 'Shelf occupation']
            for kpi_set_name in set_names:
                self.tool_box.main_calculation(set_name=kpi_set_name)
            self.tool_box.commit_results_data()
