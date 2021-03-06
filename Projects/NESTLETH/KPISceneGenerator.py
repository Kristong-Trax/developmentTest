
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.NESTLETH.Utils.KPISceneToolBox import NESTLETHSceneToolBox
from Projects.NESTLETH.Utils.KPISceneToolBox import NESTLETHSceneToolBox


class SceneGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scene_tool_box = NESTLETHSceneToolBox(self.data_provider, self.output)


    @log_runtime('Total Calculations', log_start=True)
    def scene_score(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.scene_tool_box.match_product_in_scene.empty:
            Log.warning('Match product in scene is empty for this scene')
        else:
            self.scene_tool_box.main_function()

