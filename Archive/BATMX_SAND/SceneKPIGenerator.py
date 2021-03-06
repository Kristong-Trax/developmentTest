from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.BATMX_SAND.Utils.SceneKPIToolBox import SceneToolBox


__author__ = 'Eli'


class SceneGenerator:

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = SceneToolBox(self.data_provider)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        self.tool_box.common.commit_results_data(result_entity='scene')

