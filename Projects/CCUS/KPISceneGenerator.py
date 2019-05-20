
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.CCUS.XM.Utils.KPISceneToolBox import CCUSSceneToolBox
from Projects.CCUS.Pillars.Utils.KPISceneToolBox import PillarsSceneToolBox

from KPIUtils_v2.DB.CommonV2 import Common


class SceneGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.common = Common(data_provider)
        self.scene_tool_box = CCUSSceneToolBox(self.data_provider, self.output, self.common)
        self.pillar_scene_tool_box = PillarsSceneToolBox(self.data_provider, self.output, self.common)

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
            self.pillar_scene_tool_box.is_scene_belong_to_program()
            self.common.commit_results_data(result_entity='scene')
