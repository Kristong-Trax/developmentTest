from Projects.RINIELSENUS.TYSON.Utils.KPIToolBox import TysonToolBox
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'trevaris'


class TysonGenerator:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = TysonToolBox(data_provider, output)
        self.common = Common(data_provider)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        self.tool_box.main_calculation()
        self.tool_box.commit_results()
