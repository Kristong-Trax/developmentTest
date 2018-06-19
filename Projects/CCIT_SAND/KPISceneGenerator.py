
from Trax.Utils.Logging.Logger import Log

from Projects.CCIT_SAND.Utils.KPISceneToolBox import CCITSceneToolBox

from KPIUtils_v2.DB.Common import Common

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'nissand'


class SceneGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scene_tool_box = CCITSceneToolBox(self.data_provider, self.output)
        self.common = Common(data_provider)

    @log_runtime('Total Calculations', log_start=True)
    def occupancy_calculation(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.scene_tool_box.match_product_in_scene.empty:
            Log.warning('Match product in scene is empty for this session')
        self.scene_tool_box.occupancy_calculation()
        self.common.commit_results_data()
