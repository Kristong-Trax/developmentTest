from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.GMIUS.Utils.KPIToolBox import ToolBox
from KPIUtils_v2.DB.CommonV2 import Common


__author__ = 'Sam'


class Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common = Common(self.data_provider)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = ToolBox(self.data_provider, self.output, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        if self.tool_box.scif.empty:
            Log.warning('Distribution is empty for this session')
        else:
            self.tool_box.main_calculation()
            self.common.commit_results_data()
