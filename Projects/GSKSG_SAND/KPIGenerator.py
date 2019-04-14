
from Trax.Utils.Logging.Logger import Log

from Projects.GSKSG_SAND.Utils.KPIToolBox import GSKSGToolBox

from KPIUtils_v2.DB.Common import Common

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'urid'


class Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = GSKSGToolBox(self.data_provider, self.output)
        self.common = Common(data_provider)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
        # for kpi_set_fk in self.tool_box.kpi_static_data['kpi_set_fk'].unique().tolist():
        #     score = self.tool_box.main_calculation(kpi_set_fk=kpi_set_fk)
        #     self.common.write_to_db_result(kpi_set_fk, self.tool_box.LEVEL1, score)
        self.common.commit_results_data()
