from Projects.PS3_SAND.CommonV2 import Common
from Projects.PS3_SAND.Utils.KPIToolBox import ToolBox
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Trax.Utils.Logging.Logger import Log
__author__ = 'Sam_Shivi'


class Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common = Common(self.data_provider)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = ToolBox(self.data_provider, self.output, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        if self.tool_box.scif.empty:
            Log.warning('Distribution is empty for this session')
            return
        self.run_function(self.tool_box.get_fixture_availability)
        self.run_function(self.tool_box.get_pog_store)
        self.run_function(self.tool_box.get_da_store)
        self.run_function(self.tool_box.get_facings_compliance_store)
        self.run_function(self.tool_box.get_share_of_competitor_store)
        self.run_function(self.tool_box.get_missing_denominations_store)
        self.tool_box.commit_results()

    @staticmethod
    def run_function(func):
        try:
            func()
        except Exception as e:
            Log.error('{}'.format(e))
