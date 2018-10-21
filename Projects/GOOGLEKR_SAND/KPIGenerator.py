from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.GOOGLEKR_SAND.Utils.KPIToolBox import GOOGLEToolBox
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2


__author__ = 'Eli'


class GOOGLEGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common_v2 = CommonV2(self.data_provider)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = GOOGLEToolBox(self.data_provider, self.output, self.common_v2)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        if self.tool_box.scif.empty:
            Log.warning('Distribution is empty for this session')

        self.google_global_fixture_compliance()
        # google.visit_osa_and_pog()
        self.common_v2.commit_results_data()

    def google_global_fixture_compliance(self):
        try:
            self.tool_box.google_global_fixture_compliance()
        except Exception as e:
            Log.error('{}'.format(e))

    def scene_osa_and_pog(self):
        try:
            self.tool_box.get_fixture_osa()
            self.tool_box.get_planogram_fixture_details()
        except Exception as e:
            Log.error('{}'.format(e))
