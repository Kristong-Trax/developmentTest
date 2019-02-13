
from Trax.Utils.Logging.Logger import Log

from Projects.GPUS.Utils.KPIToolBox import GPUSToolBox
from Projects.GPUS.Utils.CommonV3 import Common

# from KPIUtils_v2.DB.CommonV2 import Common

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'nicolaske'


class Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common = Common(self.data_provider)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = GPUSToolBox(self.data_provider, self.common, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
        self.common.commit_results_data()
