from Trax.Utils.Logging.Logger import Log

from Projects.INBEVCO_SAND.Utils.KPIToolBox import INBEVCO_SANDToolBox, log_runtime

__author__ = 'Israel'


class INBEVCO_SANDGenerator:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = INBEVCO_SANDToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        for kpi_set_name in self.tool_box.kpi_static_data['kpi_set_name'].unique().tolist():
            self.tool_box.main_calculation(kpi_set_name=kpi_set_name)
        self.tool_box.commit_results_data()
