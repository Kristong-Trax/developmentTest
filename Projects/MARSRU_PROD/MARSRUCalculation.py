import pandas as pd
# from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.MARSRU_PROD.Utils.MARSRUToolBox import MARSRU_PRODMARSRUKPIToolBox
from Projects.MARSRU_PROD.Utils.MARSRUJSON import MARSRU_PRODMARSRUJsonGenerator

__author__ = 'urid'


class MARSRU_PRODMARSRUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()  # use log.time_message
        tool_box = MARSRU_PRODMARSRUKPIToolBox(self.data_provider, self.output, 'MARS KPIs 2017')
        tool_box.hadle_update_custom_scif()
        jg = MARSRU_PRODMARSRUJsonGenerator('marsru-prod')
        jg.create_json('KPI MARS 23.03.18.xlsx')
        tool_box.check_availability(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.check_survey_answer(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.check_number_of_scenes(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.custom_linear_sos(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.check_price(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.brand_blocked_in_rectangle(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.custom_marsru_1(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.check_layout_size(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.golden_shelves(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.facings_by_brand(jg.project_kpi_dict.get('kpi_data')[0])
        # the order is important - KPI 2254 SHOULD be calculated after linear SOS and layout size
        tool_box.must_range_skus(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.multiple_brands_blocked_in_rectangle(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.negative_neighbors(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.get_total_linear(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.get_placed_near(jg.project_kpi_dict.get('kpi_data')[0])
        attributes_for_table1 = pd.DataFrame([(tool_box.set_name, tool_box.session_uid,
                                               tool_box.store_id, tool_box.visit_date.isoformat()
                                               , 100, 3)], columns=['kps_name', 'session_uid', 'store_fk',
                                                                       'visit_date', 'score_1',
                                                                       'kpi_set_fk'])

        tool_box.write_to_db_result(attributes_for_table1, 'level1', tool_box.set_name)
        tool_box.commit_results_data()
        self.timer.stop('MARSRU_PRODMARSRUCalculations.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('MARSRU_PROD calculations')
#     Config.init()
#     project_name = 'marsru-prod'
# #     # session_uids = ['e85aad1b-ec24-4c2e-9eae-5cac69445f98']
# #     # session_uids = ['39150053-177f-44f8-ae66-bcbc5d4b4455']  #sand
# #     # session_uids = ['536a6ca9-92d4-48cb-89b7-0e2ece07beb9']  #prod
# #     session_uids = ['9b8c7e73-3450-4b97-9792-42374aa7ef34']
# #     session_uids = ['3d9d0649-3a21-4f3e-957d-ce13f49eae86']
# #     session_uids = ['e039311c-d5c7-4cdd-b078-3d008a9da928']
#     session_uids = ['64e8b300-610d-4f1b-9c64-3f29b871a1cd']
#     # session_uids = ['64e8b300-610d-4f1b-9c64-3f29b871a1cd', 'e039311c-d5c7-4cdd-b078-3d008a9da928', '3d9d0649-3a21-4f3e-957d-ce13f49eae86',
#     #                 '9b8c7e73-3450-4b97-9792-42374aa7ef34', '536a6ca9-92d4-48cb-89b7-0e2ece07beb9']
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         MARSRU_PRODMARSRUCalculations(data_provider, output).run_project_calculations()
#
#     # jg = MARSRU_PRODMARSRUJsonGenerator('marsru-prod')
#     # jg.create_json('KPI MARS 16.03.17.xlsx')
#     # # tb = MARSRU_PRODMARSRUKPIToolBox(data_provider, self.output, 'Hypermarket')
#     # tb = MARSRU_PRODMARSRUKPIToolBox(data_provider, output, 'MARS KPIs 2017')
#     # tb.insert_new_kpis_old(project_name, jg.project_kpi_dict.get('kpi_data')[0])
#     # tb.insert_new_kpis_old(project_name)
