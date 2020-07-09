
from Trax.Utils.Logging.Logger import Log

from Projects.DIAGEOUS.Utils.menu_count.MenuUtil import MenuToolBox
from Projects.DIAGEOUS.Utils.case_count.CaseCountSKUUtil import CaseCountCalculator

from Projects.DIAGEOUS.Utils.KPIToolBox import ToolBox
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'Elyashiv'


class Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = ToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
            return
        self.tool_box.main_calculation()
        CaseCountCalculator(self.data_provider, self.tool_box.common).main_case_count_calculations()
        MenuToolBox(self.data_provider, self.tool_box.common).main_calculation()
        self.tool_box.common.commit_results_data()
