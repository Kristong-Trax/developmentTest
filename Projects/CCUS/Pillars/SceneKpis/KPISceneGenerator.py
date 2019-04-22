
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.CCUS.Pillars.Utils.KPISceneToolBox import PillarsSceneToolBox
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.DB.Common import Common as old_common

__author__ = 'Jasmine'


class SceneGenerator:

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.common_old = old_common(self.data_provider)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scene_tool_box = PillarsSceneToolBox(self.data_provider, self.common, self.common_old)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
            This is the main KPI calculation function.
            It calculates the score for every KPI set and saves it to the DB.
            Scene level.
        """

        if self.scene_tool_box.match_product_in_scene.empty:
            Log.warning('Match product in scene is empty for this scene')
        self.scene_tool_box.is_scene_belong_to_program()
        self.common.commit_results_data(result_entity='scene')


