
from Trax.Utils.Logging.Logger import Log

from Projects.MARS_CHOCO_RU_SAND.Utils.KPIToolBox import MARS_CHOCO_RU_SANDMARSToolBox, log_runtime

__author__ = 'Sanad'


class MARS_CHOCO_RU_SANDMARSGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = MARS_CHOCO_RU_SANDMARSToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        for kpi_set_fk in self.tool_box.kpi_static_data['kpi_set_fk'].unique().tolist():
             self.tool_box.main_calculation(kpi_set_fk=kpi_set_fk)
             self.tool_box.write_to_db_result(kpi_set_fk, 0, self.tool_box.LEVEL1)
        for kpi_fk in self.tool_box.kpi_static_data['kpi_fk'].unique().tolist():
            self.tool_box.write_to_db_result(kpi_fk, 0, self.tool_box.LEVEL2)
        self.tool_box.commit_results_data()
