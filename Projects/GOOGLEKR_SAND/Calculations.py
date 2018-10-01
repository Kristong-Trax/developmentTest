from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

# from KPIUtils.GlobalProjects.HEINZ.KPIGenerator import HEINZGenerator
from Projects.GOOGLEKR_SAND.KPIGenerator import GOOGLEGenerator

# from KPIUtils.DB.Common_V2 import Common as Common2
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2

import pandas as pd

__author__ = 'Eli'


class GOOGLEKR_SANDCalculations(BaseCalculationsScript):
    """
    https://confluence.trax-cloud.com/pages/resumedraft.action?draftId=174198555&draftShareId=feac8c7a-ec57-4b36-b380-190d3668debc
    """
    def run_project_calculations(self):
        self.timer.start()
        common = CommonV2(self.data_provider)
        google = GOOGLEGenerator(self.data_provider, self.output, common)


        # heinz.heinz_global_distribution_per_category()
        # heinz.heinz_global_share_of_shelf_function()
        # heinz.heinz_global_price_adherence(pd.read_excel("/root/KEngine/prod_KPI/Projects/HEINZCR_SAND/Config/PriceAdherenceTargets010718.xlsx", sheetname="Price Adherence"))
        # heinz.heinz_global_extra_spaces()
        # common.commit_results_data_to_new_tables()
        # common.commit_results_data()

        for scene in google.tool_box.scene_info['scene_fk']:
            common.scene_id = scene
            google.tool_box.scif = google.tool_box.scif[google.tool_box.scif['scene_id'] == scene]

            google.google_global_SOS()


        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('googlekr-sand calculations')
    Config.init()
    project_name = 'googlekr-sand'
    data_provider = KEngineDataProvider(project_name)
    # session = 'efdd2028-6f09-46ff-ad02-18874a6f45b2'
    sessions = ['0002f34c-c186-11e8-b150-12499d9ea556']
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        GOOGLEKR_SANDCalculations(data_provider, output).run_project_calculations()
