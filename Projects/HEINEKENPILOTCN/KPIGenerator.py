
from Trax.Utils.Logging.Logger import Log
from Projects.HEINEKENPILOTCN.Utils.ToolBox import HEINEKENPILOTCNHandleTemplate
from Projects.HEINEKENPILOTCN.Utils.KPIToolBox import HEINEKENPILOTCNToolBox, log_runtime

__author__ = 'Yasmin'


class HEINEKENPILOTCNGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = HEINEKENPILOTCNToolBox(self.data_provider, self.output)
        self.template_tool = HEINEKENPILOTCNHandleTemplate(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        log_runtime('Updating templates')(self.template_tool.update_templates)()
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
        self.tool_box.commit_results_data()
