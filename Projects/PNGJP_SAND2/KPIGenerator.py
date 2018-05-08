
from Trax.Utils.Logging.Logger import Log

from Projects.PNGJP_SAND2.Utils.KPIToolBox import PNGJP_SAND2ToolBox, log_runtime
from Projects.PNGJP_SAND2.Utils.KpiQualitative import PNGJP_SAND2KpiQualitative_ToolBox, log_runtime


__author__ = 'Nimrod'


class PNGJP_SAND2Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = PNGJP_SAND2ToolBox(self.data_provider, self.output)
        self.KpiQualitative_tool_box = PNGJP_SAND2KpiQualitative_ToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
        self.tool_box.hadle_update_custom_scif()
        self.tool_box.commit_results_data()
        self.KpiQualitative_tool_box.main_calculation()
        self.KpiQualitative_tool_box.commit_results_data()


