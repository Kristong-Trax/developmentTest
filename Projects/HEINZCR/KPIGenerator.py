
from Trax.Utils.Logging.Logger import Log

from Projects.HEINZCR.Utils.KPIToolBox import HEINZCRToolBox

__author__ = 'Eli'


class HEINZCRGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = HEINZCRToolBox(self.data_provider, self.output)

    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
        self.tool_box.commit_results_data()
