
from Trax.Utils.Logging.Logger import Log

from Projects.CCIT_SAND.Utils.KPISceneToolBox import CCITSceneToolBox

from KPIUtils_v2.DB.CommonV2 import Common

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'nissand'


class SceneGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.common = Common(data_provider)
        self.scene_tool_box = CCITSceneToolBox(self.data_provider, self.output, self.common)


    @log_runtime('Total Calculations', log_start=True)
    def scene_score(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.scene_tool_box.match_product_in_scene.empty:
            Log.warning('Match product in scene is empty for this scene')
        else:
            self.scene_tool_box.scene_score()
            self.common.commit_results_data(by_scene=True)

