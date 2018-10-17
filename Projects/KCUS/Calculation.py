from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from mock import MagicMock
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.KCUS.Utils.KCUSToolBox import KCUSToolBox
from Projects.KCUS.Utils.KCUSJSON import KCUSJsonGenerator
from Projects.KCUS_SAND.Utils.KpiToolbox import KCUS_KPIToolBox

__author__ = 'ortalk'



class KCUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        tool_box = KCUSToolBox(self.data_provider, self.output, set_name='KC Score')
        self.timer.stop('KPIGenerator.run_project_calculations')
        jg = KCUSJsonGenerator('kc_us-prod')
        jg.create_json('KEngine_KC_US.xlsx')
        for p in jg.project_kpi_dict['Block Together']:
            if p.get('KPI_Type') == 'Relative position of Blocked Together':
                tool_box.calculate_block_together_relative(p)
            if p.get('KPI_Type') == 'Blocked Together':
                tool_box.calculate_block_together(p)
        for p in jg.project_kpi_dict['Simple KPIs']:
            if p.get('KPI_Type') == 'Anchor':
                tool_box.calculate_anchor(p)
            if p.get('KPI_Type') == 'Eye Level':
                tool_box.calculate_eye_level(p)
            if p.get('KPI_Type') == 'Flow':
                tool_box.calculate_flow(p)
            # if p.get('KPI_Type') == 'Survey':
            #     tool_box.check_survey_answer(p)
            # if p.get('KPI_Type') == 'Flow between':
            #     tool_box.calculate_flow_between(p)
        for p in jg.project_kpi_dict['Relative Position']:
            if p.get('KPI_Type') == 'Relative Position':
                tool_box.calculate_relative_position(p)
            if p.get('KPI_Type') == 'Assortment':
                tool_box.calculate_assort(p)


        tool_box.commit_results_data()

        kpitool_box = KCUS_KPIToolBox(self.data_provider, self.output, set_name='KC Score')
        kpitool_box.main_calculation()
        kpitool_box.commit_results_data()

        Log.info('calculate kpi took {}'.format(tool_box.download_time))

# if __name__ == '__main__':
#     LoggerInitializer.init('kcus-prod calculations')
#     Config.init()
#     project_name = 'kcus'
#     sessions = ['3472034a-3f2c-47d8-b390-26c2bacbadbf']
#
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#         data_provider.load_session_data(session)
#         output = Output()
#         KCUSCalculations(data_provider, output).run_project_calculations()
