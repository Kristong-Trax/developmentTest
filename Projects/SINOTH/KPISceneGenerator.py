from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common
from Projects.SINOTH.Utils.KPISceneToolBox import SINOTHSceneToolBox
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime


class SceneGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.common = Common(data_provider)
        self.scene_tool_box = SINOTHSceneToolBox(self.data_provider, self.output, self.common)


    @log_runtime('Total Calculations', log_start=True)
    def scene_main_calculation(self):
        """
        This is the main KPI calculation function.
        """
        if self.scene_tool_box.match_product_in_scene.empty:
            Log.warning('Match product in scene is empty for this scene')
        else:
            self.scene_tool_box.filter_and_send_kpi_to_calc()
            self.common.commit_results_data(result_entity='scene')
