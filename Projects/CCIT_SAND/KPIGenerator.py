
from Trax.Utils.Logging.Logger import Log

from Projects.CCIT_SAND.Utils.KPIToolBox import CCITToolBox

from KPIUtils_v2.DB.Common import Common as commonV1
from KPIUtils_v2.DB.CommonV2 import Common as commonV2

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'nissand'


class Generator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.commonV1 = commonV1(data_provider)
        self.commonV2 = commonV2(data_provider)
        self.tool_box = CCITToolBox(self.data_provider, self.output, self.commonV2, self.commonV1)


    @log_runtime('Total Calculations', log_start=True)
    def main_calculation(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        else:
            self.tool_box.main_function()
            self.commonV1.commit_results_data()
            self.commonV2.commit_results_data(scene_session_hierarchy=True)
