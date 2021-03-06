
from Trax.Utils.Logging.Logger import Log

from Projects.MARSUAE.Utils.KPIToolBox import MARSUAEToolBox

from KPIUtils_v2.DB.CommonV2 import Common

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'natalyak'


class MARSUAEGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = MARSUAEToolBox(self.data_provider, self.output)
        # self.common = Common(data_provider)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.data_provider.scenes_info.empty:
            Log.warning('Session without scenes. Kpis will not be calcualated')
            return
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')

        self.tool_box.main_calculation()
        self.tool_box.common.commit_results_data()