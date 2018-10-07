from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.GOOGLEKR_SAND.KPIGenerator import GOOGLEGenerator
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from Trax.Utils.Logging.Logger import Log
import pandas as pd

__author__ = 'Eli_Shivi_Sam'


class GOOGLEKR_SANDCalculations(BaseCalculationsScript):
    """
    https://confluence.trax-cloud.com/pages/resumedraft.action?draftId=174198555&draftShareId=feac8c7a-ec57-4b36-b380-190d3668debc
    """
    def run_project_calculations(self):
        self.timer.start()
        common_v2 = CommonV2(self.data_provider)
        google = GOOGLEGenerator(self.data_provider, self.output, common_v2)
        # ' Session Level KPIs'
        if google.tool_box.scif.empty:
            Log.warning('Distribution is empty for this session')
        google.google_global_fixture_compliance()
        google.google_global_survey()
        google.visit_osa_and_pog()
        common_v2.commit_results_data()
        for scene in google.tool_box.scene_info['scene_fk']:
            google.common_v2.kpi_results = pd.DataFrame(columns=google.common_v2.COLUMNS)
            google.common_v2.scene_id = scene
            google.tool_box.scif = google.tool_box.scif[google.tool_box.scif['scene_id'] == scene]
            # 'Scene Level KPIs'
            google.google_global_SOS()
            google.scene_osa_and_pog()
            google.common_v2.commit_results_data(result_entity='scene')
        self.timer.stop('KPIGenerator.run_project_calculations')
