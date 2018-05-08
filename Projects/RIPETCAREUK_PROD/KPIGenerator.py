
from Trax.Utils.Logging.Logger import Log
from Projects.RIPETCAREUK_PROD.Utils.KPIToolBox import MarsUkPerfectScore
from Projects.RIPETCAREUK_PROD.Utils.Utils import log_runtime

__author__ = 'Dudi S'


class MarsUkGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = MarsUkPerfectScore(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.data_provider.scene_item_facts.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.calculate()


