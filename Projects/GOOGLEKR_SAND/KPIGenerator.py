from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.GOOGLEKR_SAND.Utils.KPIToolBox import GOOGLEToolBox

__author__ = 'Eli'


class GOOGLEGenerator:

    def __init__(self, data_provider, output, common_v2):
        self.data_provider = data_provider
        self.output = output
        self.common_v2 = common_v2
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = GOOGLEToolBox(self.data_provider, self.output, self.common_v2)

    @log_runtime('Total Calculations', log_start=True)
    def google_global_SOS(self):
        try:
            # Log.info('In KPI generator GOOGLE-SAND')
            if self.tool_box.scif.empty:
                Log.warning('Distribution is empty for this session')
            self.tool_box.google_global_SOS()
        except Exception as e:
            Log.error('{}'.format(e))

    def google_global_fixture_compliance(self):
        try:
            # Log.info('In KPI generator GOOGLE-SAND')
            if self.tool_box.scif.empty:
                Log.warning('Distribution is empty for this session')
            self.tool_box.google_global_fixture_compliance()
        except Exception as e:
            Log.error('{}'.format(e))

    def google_global_survey(self):
        try:
            # Log.info('In KPI generator GOOGLE-SAND')
            if self.tool_box.scif.empty:
                Log.warning('Distribution is empty for this session')
            self.tool_box.google_global_survey()
        except Exception as e:
            Log.error('{}'.format(e))

