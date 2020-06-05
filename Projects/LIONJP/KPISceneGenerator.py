from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.LIONJP.Utils.KPISceneToolBox import LionJPSceneToolBox


class SceneGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.common = Common(data_provider)
        self.scene_tool_box = LionJPSceneToolBox(self.data_provider, self.output, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def scene_main_calculation(self):
        """
        This is the main KPI calculation function.
        """
        if self.scene_tool_box.match_product_in_scene.empty:
            Log.warning('Match product in scene is empty for this scene')
        else:
            self.scene_tool_box.main_calculation()
            self.common.commit_results_data(result_entity='scene')
