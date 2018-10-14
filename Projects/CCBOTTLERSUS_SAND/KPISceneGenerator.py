
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.CCBOTTLERSUS_SAND.REDSCORE.SceneKPIBox import CCBOTTLERSUS_SANDSceneRedToolBox
from Projects.CCBOTTLERSUS_SAND.CMA_SOUTHWEST.SceneKPIToolBox import CCBOTTLERSUS_SANDSceneCokeCoolerToolbox
from KPIUtils_v2.DB.CommonV2 import Common


class SceneGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.common = Common(data_provider)

    @log_runtime('Total CCBOTTLERSUS_SANDSceneCalculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        self.calculate_scene_redscore()
        self.calculate_scene_coke_cooler()

        self.common.commit_results_data(result_entity='scene')

    @log_runtime('Scene RedScore Calculations', log_start=True)
    def calculate_scene_redscore(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        tool_box = CCBOTTLERSUS_SANDSceneRedToolBox(self.data_provider, self.output, self.common)
        if tool_box.match_product_in_scene.empty:
            Log.warning('Match product in scene is empty for this scene')
        else:
            if tool_box.main_calculation():
                pass
                # self.common.commit_results_data(result_entity='scene')

    @log_runtime('Scene Coke Cooler Calculations', log_start=True)
    def calculate_scene_coke_cooler(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        tool_box = CCBOTTLERSUS_SANDSceneCokeCoolerToolbox(self.data_provider, self.output, self.common)
        if tool_box.match_product_in_scene.empty:
            Log.warning('Match product in scene is empty for this scene')
        else:
            if tool_box.main_calculation():
                pass
                # self.common.commit_results_data(result_entity='scene')
