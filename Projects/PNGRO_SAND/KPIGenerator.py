
from Trax.Utils.Logging.Logger import Log

from Projects.PNGRO_SAND.Utils.KPIToolBox import PNGRO_SAND_PRODToolBox, log_runtime

__author__ = 'Israel'


class PNGRO_SAND_PRODGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = PNGRO_SAND_PRODToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """

        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
        if self.tool_box.main_shelves:
            for kpi_set_fk in self.tool_box.kpi_static_data['kpi_set_fk'].unique().tolist():
                self.tool_box.write_to_db_result(kpi_set_fk, self.tool_box.LEVEL1, 100)
            for kpi_fk in self.tool_box.kpi_static_data['kpi_fk'].unique().tolist():
                self.tool_box.write_to_db_result(kpi_fk, self.tool_box.LEVEL2, 100)
        # for kpi_set_fk in self.tool_box.kpi_static_data['kpi_set_fk'].unique().tolist():
        #     self.tool_box.write_to_db_result(kpi_set_fk, self.tool_box.LEVEL1, 100)
        # for kpi_fk in self.tool_box.kpi_static_data['kpi_fk'].unique().tolist():
        #     self.tool_box.write_to_db_result(kpi_fk, self.tool_box.LEVEL2, 100)
        self.tool_box.commit_results_data()
        self.tool_box.common.commit_results_data()
        # self.tool_box.common.commit_results_data_to_new_tables()
