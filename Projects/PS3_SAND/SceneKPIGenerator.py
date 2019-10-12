from Projects.PS3_SAND.CommonV2 import Common
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.PS3_SAND.Utils.SceneKPIToolBox import SceneToolBox
from Trax.Utils.Logging.Logger import Log


__author__ = 'Eli'


class SceneGenerator:

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = SceneToolBox(self.data_provider, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        self.run_function(self.tool_box.did_pass_rule)
        self.run_function(self.tool_box.google_global_sos)
        self.run_function(self.tool_box.get_fixture_da)
        self.run_function(self.tool_box.get_planogram_fixture_level)
        self.run_function(self.tool_box.get_sos_target)
        self.run_function(self.tool_box.get_above_the_fold)
        self.run_function(self.tool_box.get_facings_compliance)
        self.tool_box.commit_results()

    @staticmethod
    def run_function(func):
        try:
            func()
        except Exception as e:
            Log.error('{}'.format(e))
