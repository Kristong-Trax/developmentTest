
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.MONDELEZUSPS.Utils.KPISceneToolBox import SceneToolBox

__author__ = 'krishnat'


class SceneGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scene_tool_box = SceneToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def scene_score(self):
        if self.scene_tool_box.matches.empty:
            Log.warning('Match product in scene is empty for this scene')
        self.scene_tool_box.main_function()
        self.scene_tool_box.commit_results()
