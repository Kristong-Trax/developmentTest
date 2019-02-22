
from Trax.Utils.Logging.Logger import Log

from Projects.DIAGEOGH.Utils.KPIToolBox import DIAGEOGHToolBox, log_runtime

__author__ = 'Yasmin'


class DiageoGHGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = DIAGEOGHToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        # log_runtime('Updating templates')(self.tool_box.tools.update_templates)()
        self.tool_box.tools.update_templates()
        set_names = self.tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
        self.tool_box.main_calculation(set_names=set_names)
        self.tool_box.commit_results_data()
