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
        common_v2 = CommonV2(self.data_provider)
        google = GOOGLEGenerator(self.data_provider, self.output, common_v2)

        ' Session Level KPIs'
        google.google_global_fixture_compliance()
        google.google_global_survey()

        common_v2.commit_results_data()

        for scene in google.tool_box.scene_info['scene_fk']:
            google.common_v2.kpi_results = pd.DataFrame(columns=google.common_v2.COLUMNS)
            google.common_v2.scene_id = scene
            google.tool_box.scif = google.tool_box.scif[google.tool_box.scif['scene_id'] == scene]

            'Scene Level KPIs'
            google.google_global_SOS()

            google.common_v2.commit_results_data(result_entity='scene')

        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('googlekr-sand calculations')
    Config.init()
    project_name = 'googlekr-sand'
    data_provider = KEngineDataProvider(project_name)
    # session = 'efdd2028-6f09-46ff-ad02-18874a6f45b2'
    sessions = [
                # '0002f34c-c186-11e8-b150-12499d9ea556',  # empty session for error testing
                '00581f3d-c25d-11e8-b150-12499d9ea556',
                '0ad9a74c-c2dc-11e8-b150-12499d9ea556',
                '0d3f9fc8-c155-11e8-b150-12499d9ea556',
                # '15578a99-c186-11e8-b150-12499d9ea556',
                # '16bb102b-c0bb-11e8-b150-12499d9ea556',
                # '1718cb09-c25d-11e8-b150-12499d9ea556',
                # '1b7ecd17-c095-11e8-b150-12499d9ea556',
                # '2c3cceaf-c25d-11e8-b150-12499d9ea556',
                # '2c4fda9b-c2dc-11e8-b150-12499d9ea556',
                # '365a20f8-c2dc-11e8-b150-12499d9ea556',
                # '3853cb53-c20e-11e8-b150-12499d9ea556',
                # '3d0b9c1a-bb2b-11e8-b150-12499d9ea556',
                # '4f6b4ced-c2dc-11e8-b150-12499d9ea556',
                # '5951864c-c25d-11e8-b150-12499d9ea556',
                # '5adfc053-c2dc-11e8-b150-12499d9ea556',
                # '5df38355-c185-11e8-b150-12499d9ea556',
                # '67c1a58e-c2dc-11e8-b150-12499d9ea556',
                # '6c0cf9c5-c20e-11e8-b150-12499d9ea556',
                # '71e6adc6-c153-11e8-b150-12499d9ea556',
                # '72f45bce-c185-11e8-b150-12499d9ea556',
                # '779b6879-c0a3-11e8-b150-12499d9ea556',
                # '77cc255d-c22b-11e8-b150-12499d9ea556',
                # '81bbfa91-c25d-11e8-b150-12499d9ea556',
                # '857ef877-c2dc-11e8-b150-12499d9ea556',
                # '8b394c78-c185-11e8-b150-12499d9ea556',
                # '8e79c979-c25c-11e8-b150-12499d9ea556',
                # '949d62f4-c145-11e8-b150-12499d9ea556',
                # '982eaace-c2dc-11e8-b150-12499d9ea556',
                # 'a17f09cb-c185-11e8-b150-12499d9ea556',
                # 'a30e85d1-c2dc-11e8-b150-12499d9ea556',
                # 'a4b2313c-c25c-11e8-b150-12499d9ea556',
                # 'b230ce14-c2dc-11e8-b150-12499d9ea556',
                # 'b730d6fe-c185-11e8-b150-12499d9ea556',
                # 'bbd2d9aa-c25c-11e8-b150-12499d9ea556',
                # 'c143a21a-c142-11e8-b150-12499d9ea556',
                # 'c732de44-c08e-11e8-b150-12499d9ea556',
                # 'ca867d7c-c185-11e8-b150-12499d9ea556',
                # 'cfe40e0d-c153-11e8-b150-12499d9ea556',
                # 'd20a2bdb-c25c-11e8-b150-12499d9ea556',
                # 'd3be10bf-c093-11e8-b150-12499d9ea556',
                # 'db3ed661-c146-11e8-b150-12499d9ea556',
                # 'e78388ea-c25c-11e8-b150-12499d9ea556',
                # 'ec0108db-c185-11e8-b150-12499d9ea556',
                ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        GOOGLEKR_SANDCalculations(data_provider, output).run_project_calculations()
