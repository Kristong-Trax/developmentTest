
from Trax.Utils.Logging.Logger import Log

from Projects.DIAGEODISPUS_SAND.Utils.KPIToolBox import DIAGEODISPUSToolBox

from KPIUtils_v2.DB.Common import Common

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'nissand'


class Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.common = Common(data_provider)
        self.tool_box = DIAGEODISPUSToolBox(self.data_provider, self.output, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
        self.common.commit_results_data_to_new_tables()
