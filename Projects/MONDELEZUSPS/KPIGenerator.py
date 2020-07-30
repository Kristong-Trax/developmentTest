from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.MONDELEZUSPS.Utils.KPIToolBox import ToolBox
from Projects.MONDELEZUSPS.DMI.KPIToolBox import DMIToolBox

__author__ = 'krishnat'


class Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.common = CommonV2(self.data_provider)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """

        self.calculate_mondelezusps()
        self.calculate_dmi()
        self.common.commit_results_data()

    def calculate_mondelezusps(self):
        tool_box = ToolBox(self.data_provider, self.output, self.common)
        if tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        tool_box.main_calculation()

    @log_runtime('DMI calculations')
    def calculate_dmi(self):
        tool_box = DMIToolBox(self.data_provider, self.output, self.common)
        tool_box.main_calculation()
