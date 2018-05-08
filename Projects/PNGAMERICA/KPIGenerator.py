
from Trax.Utils.Logging.Logger import Log

from Projects.PNGAMERICA.Utils.KPIToolBox import PNGAMERICAToolBox, log_runtime

__author__ = 'Ortal'


class PNGAMERICAGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = PNGAMERICAToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
            return
        # pass
        for kpi_set_fk in [1,2,3,4,5,6,7,9,10,11,12,13,14,15,16,17,18,21,22,23,24,25,26,27,28,29,30,31,32,33,36,37,38,
                           39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54]:
        # for kpi_set_fk in [46, 47, 48, 49, 50]:
        # for kpi_set_fk in self.tool_box.kpi_static_data['kpi_set_fk'].unique().tolist():
        #     set_name = self.tool_box.kpi_static_data['kpi_set_fk'] == kpi_set_fk
            self.tool_box.main_calculation(kpi_set_fk=kpi_set_fk)
            self.tool_box.write_to_db_result(kpi_set_fk, result=None, level=self.tool_box.LEVEL1)
            set_kpis = self.tool_box.kpi_static_data.loc[self.tool_box.kpi_static_data[
                                                             'kpi_set_fk'] == kpi_set_fk]['kpi_fk'].unique().tolist()
            for kpi in set_kpis:
                self.tool_box.write_to_db_result(kpi_set_fk, result=None, level=self.tool_box.LEVEL2, kpi_fk=kpi)
        # self.tool_box.calculate_auto_assortment_compliance()
        self.tool_box.calculate_auto_assortment_compliance_per_category()
        self.tool_box.commit_results_data()
