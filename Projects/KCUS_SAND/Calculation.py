from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from mock import MagicMock
from Trax.Utils.Conf.Configuration import Config

from Projects.KCUS_SAND.Utils.KpiToolbox import KCUS_SAND_KPIToolBox
from Projects.KCUS_SAND.Utils.KCUS_SANDJSON import KCUS_SANDJsonGenerator

__author__ = 'ortalk'


class KCUS_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        # pass
        self.timer.start()
        tool_box = KCUS_SAND_KPIToolBox(self.data_provider, self.output, set_name='KC Score')
        tool_box.main_calculation()
        self.timer.stop('KPIGenerator.run_project_calculations')
        # jg = KCUS_SANDJsonGenerator('KEngine_KC_US')
        # jg.create_json('KEngine_KC_US.xlsx')
        # for p in jg.project_kpi_dict['Block Together']:
        #     try:
        #         if p.get('KPI_Type') == 'Relative position of Blocked Together':
        #             tool_box.calculate_block_together_relative(p)
        #         if p.get('KPI_Type') == 'Blocked Together':
        #             tool_box.calculate_block_together(p)
        #     except Exception as e:
        #         Log.info('KPI {} failed due to {}'.format(p.get('KPI Level 2 Name'), e))
        #         continue
        # for p in jg.project_kpi_dict['Simple KPIs']:
        #     try:
        #         if p.get('KPI_Type') == 'Anchor':
        #             tool_box.calculate_anchor(p)
        #         if p.get('KPI_Type') == 'Eye Level':
        #             tool_box.calculate_eye_level(p)
        #         if p.get('KPI_Type') == 'Flow':
        #             tool_box.calculate_flow(p)
        #         if p.get('KPI_Type') == 'Assortment':
        #             tool_box.calculate_assort(p)
        #         # if p.get('KPI_Type') == 'Survey':
        #         #     tool_box.check_survey_answer(p)
        #         if p.get('KPI_Type') == 'Flow between':
        #             tool_box.calculate_flow_between(p)
        #     except Exception as e:
        #         Log.info('KPI {} failed due to {}'.format(p.get('KPI Level 2 Name'), e))
        #         continue
        # for p in jg.project_kpi_dict['Relative Position']:
        #     try:
        #         tool_box.calculate_relative_position(p)
        #     except Exception as e:
        #         Log.info('KPI {} failed due to {}'.format(p.get('KPI Level 2 Name'), e))
        #         continue
        tool_box.commit_results_data()
#
# if __name__ == '__main__':
#     LoggerInitializer.init('kcus-sand calculations')
#     Config.init()
#     project_name = 'kcus'
#     sessions = ['614fd208-6078-469b-9251-a5a751b3bf05' ]
#
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#         data_provider.load_session_data(session)
#         output = Output()
#         KCUS_SANDCalculations(data_provider, output).run_project_calculations()
