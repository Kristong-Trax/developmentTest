
from Trax.Utils.Logging.Logger import Log

from Projects.CCZA.Utils.KPIToolBox import CCZAToolBox
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.CCZA.Utils.Const import Const

__author__ = 'Elyashiv'


class CCZAGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = CCZAToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.error('Scene item facts is empty for this session')
            return
        self.tool_box.main_calculation_red_score()
        self.tool_box.sos_main_calculation()
        self.tool_box.common.commit_results_data()
        self.tool_box.common_v2.commit_results_data()
