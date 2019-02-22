from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from KPIUtils.GlobalProjects.GOOGLE.Utils.SceneKPIToolBox import SceneToolBox
# from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils.GlobalProjects.GOOGLE.CommonV2 import Common

__author__ = 'Eli'


class SceneGOOGLEJP_SANDGenerator:

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = SceneToolBox(self.data_provider, self.common)

    @log_runtime('Total GOOGLEJP_SANDCalculations', log_start=True)
    def main_function(self):
        self.google_global_sos()
        self.scene_osa_and_pog()
        self.common.commit_results_data(result_entity='scene')

    def google_global_sos(self):
        try:
            self.tool_box.google_global_SOS()
        except Exception as e:
            Log.error('{}'.format(e))

    def scene_osa_and_pog(self):
        try:
            self.tool_box.get_fixture_osa()
            self.tool_box.get_planogram_fixture_details()
        except Exception as e:
            Log.error('{}'.format(e))
