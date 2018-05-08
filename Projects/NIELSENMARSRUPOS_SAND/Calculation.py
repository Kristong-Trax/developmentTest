import pandas as pd
import os

# from Trax.Utils.Conf.Configuration import Config
# # from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from KPIUtils.GlobalProjects.MARSRU.Utils.ToolBox import GLOBAL_MARSRU_KPIToolBox
from KPIUtils.GlobalProjects.MARSRU.Utils.JSON import GLOBAL_MARSRU_JsonGenerator

__author__ = 'urid'


class NIELSENMARSRUPOS_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()  # use log.time_message
        base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data')
        tool_box = GLOBAL_MARSRU_KPIToolBox(self.data_provider, self.output,
                                            set_name='NIELSENMARSRU KPIs 2018', base_path=base_path)
        tool_box.hadle_update_custom_scif()
        jg = GLOBAL_MARSRU_JsonGenerator('nielsenmarsrupos-sand', base_path)
        jg.create_json('template.xlsx')
        tool_box.check_availability(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.custom_linear_sos(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.brand_blocked_in_rectangle(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.custom_marsru_1(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.check_layout_size(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.golden_shelves(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.facings_by_brand(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.multiple_brands_blocked_in_rectangle(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.negative_neighbors(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.get_total_linear(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.get_placed_near(jg.project_kpi_dict.get('kpi_data')[0])
        # tool_box.check_survey_answer(jg.project_kpi_dict.get('kpi_data')[0])
        # tool_box.check_number_of_scenes(jg.project_kpi_dict.get('kpi_data')[0])
        # tool_box.check_price(jg.project_kpi_dict.get('kpi_data')[0])
        # tool_box.must_range_skus(jg.project_kpi_dict.get('kpi_data')[0])
        attributes_for_table1 = pd.DataFrame([(tool_box.set_name, tool_box.session_uid,
                                               tool_box.store_id, tool_box.visit_date.isoformat()
                                               , 100, 3)], columns=['kps_name', 'session_uid', 'store_fk',
                                                                       'visit_date', 'score_1',
                                                                       'kpi_set_fk'])

        tool_box.write_to_db_result(attributes_for_table1, 'level1', tool_box.set_name)
        # tool_box.commit_results_data()
        self.timer.stop('NIELSENMARSRUPOS_SANDCalculations.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('nielsenmarsrupos-sand calculations')
#     Config.init()
#     project_name = 'nielsenmarsrupos-sand'
#     session_uids = ['555e59e6-8cc3-11e7-8bba-12ae84a7e6ca']
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         NIELSENMARSRUPOS_SANDCalculations(data_provider, output).run_project_calculations()
#
#     # jg = NIELSENMARSRUPOS_SANDJsonGenerator('nielsenmarsrupos-sand')
#     # jg.create_json('template.xlsx')
#     # # tb = NIELSENMARSRUPOS_SANDMARSRUKPIToolBox(data_provider, self.output, 'Hypermarket')
#     # tb = NIELSENMARSRUPOS_SANDKPIToolBox(data_provider, output, 'NIELSENMARSRU KPIs 2018')
#     # tb.insert_new_kpis_old(project_name, jg.project_kpi_dict.get('kpi_data')[0])
#     # tb.insert_new_kpis_old(project_name)
