
from Trax.Utils.Logging.Logger import Log

from Projects.MONDELEZUS.Utils.KPIToolBox import MONDELEZUSToolBox
from Projects.MONDELEZUS.CStore.KPIToolBox import CSTOREToolBox

from KPIUtils_v2.DB.CommonV2 import Common

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'nicolaske, sam'


class Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output

        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.common = Common(data_provider)
        self.tool_box = MONDELEZUSToolBox(self.data_provider, self.output, self.common)
        self.cstore_tool_box = CSTOREToolBox(self.data_provider, self.output, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.cstore_tool_box.main_calculation()
        # for kpi_set_fk in self.tool_box.kpi_new_static_data['pk'].unique().tolist():
        #     self.tool_box.main_calculation(kpi_set_fk=kpi_set_fk)
        self.common.commit_results_data()

