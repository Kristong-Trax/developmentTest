from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.PNGHK.Utils.SceneKPIToolBox import SceneToolBox
from KPIUtils_v2.DB.CommonV2 import Common

__author__ = 'ilays'


class SceneGenerator:

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = SceneToolBox(self.data_provider, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
            This is the main KPI calculation function.
            It calculates the score for every KPI set and saves it to the DB.
            Scene level.
        """
        self.tool_box.main_calculation()
        self.common.commit_results_data(result_entity='scene')
