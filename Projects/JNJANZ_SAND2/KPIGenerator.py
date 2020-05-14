
from Trax.Utils.Logging.Logger import Log

from Projects.JNJANZ_SAND.Utils.KPIToolBox import JNJANZToolBox, log_runtime

__author__ = 'ilays'


class JNJANZGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = JNJANZToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        for kpi_set_fk in self.tool_box.kpi_static_data['kpi_set_fk'].unique().tolist():
            score = self.tool_box.main_calculation(kpi_set_fk=kpi_set_fk)
            self.tool_box.write_to_db_result(kpi_set_fk, score, self.tool_box.LEVEL1)
        self.tool_box.commit_results_data()
