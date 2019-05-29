
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.PNGCN_SAND.SceneKpis.KPISceneToolBox import PngcnSceneKpis
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.CommonV2 import Common



class SceneGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scene_id = self.data_provider.scene_id
        self.common = Common(data_provider)
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.scene_tool_box = PngcnSceneKpis(self.rds_conn, self.common, self.scene_id, self.data_provider)


    @log_runtime('Total Calculations', log_start=True)
    def PngcnSceneKpis(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        self.scene_tool_box.process_scene()



