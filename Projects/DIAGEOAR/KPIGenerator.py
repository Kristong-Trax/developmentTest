
from Trax.Utils.Logging.Logger import Log

from Projects.DIAGEOAR.Utils.KPIToolBox import DIAGEOARDIAGEOARToolBox, log_runtime

__author__ = 'Yasmin'


class DIAGEOARDIAGEOARGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = DIAGEOARDIAGEOARToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        set_names = self.tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
        calculate_activation_standard = False
        if self.tool_box.ACTIVATION_STANDARD in set_names:
            set_names.remove(self.tool_box.ACTIVATION_STANDARD)
            calculate_activation_standard = True
        self.tool_box.main_calculation(set_names=set_names)
        if calculate_activation_standard:
            self.tool_box.calculate_activation_standard()
        self.tool_box.commit_results_data()
