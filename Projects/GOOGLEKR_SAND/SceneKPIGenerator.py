from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.GOOGLEKR_SAND.Utils.SceneKPIToolBox import SceneGOOGLEToolBox
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2

__author__ = 'Eli'


class SceneGenerator:

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.common_v2 = CommonV2(self.data_provider)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = SceneGOOGLEToolBox(self.data_provider, self.common_v2)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        self.google_global_sos()
        self.scene_osa_and_pog()
        self.common_v2.commit_results_data(result_entity='scene')

    def google_global_sos(self):
        try:
            self.tool_box.google_global_SOS()
        except Exception as e:
            Log.error('{}'.format(e))

    def scene_osa_and_pog(self):
        try:
            if self.tool_box.get_fixture_osa():
                self.tool_box.get_planogram_fixture_details()
        except Exception as e:
            Log.error('{}'.format(e))
