
from Trax.Utils.Logging.Logger import Log

from Projects.INBEVCI_SAND.Utils.KPIToolBox import INBEVCISANDToolBox, log_runtime
from Projects.INBEVCI_SAND.Utils.Const import INBEVCISANDConst as Const

__author__ = 'Elyashiv'


class INBEVCISANDGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = INBEVCISANDToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session: {}'.format(self.session_uid))
            return
        for set_name in Const.SET_NAMES:
            self.tool_box.main_calculation(set_name=set_name)
        self.tool_box.common.commit_results_data_to_new_tables()
