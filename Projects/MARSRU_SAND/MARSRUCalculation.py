import pandas as pd
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Trax.Utils.Conf.Configuration import Config
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
from Projects.MARSRU_SAND.Utils.MARSRUToolBox import MARSRU_SANDMARSRUKPIToolBox
from Projects.MARSRU_SAND.Utils.MARSRUJSON import MARSRU_SANDMARSRUJsonGenerator

__author__ = 'urid'


class MARSRU_SANDMARSRUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()  # use log.time_message
        tool_box = MARSRU_SANDMARSRUKPIToolBox(self.data_provider, self.output, 'MARS KPIs 2017')
        jg = MARSRU_SANDMARSRUJsonGenerator('marsru-prod')
        jg.create_json('KPI MARS 16.03.17.xlsx')
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
        tool_box.must_range_skus(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.multiple_brands_blocked_in_rectangle(jg.project_kpi_dict.get('kpi_data')[0])

        attributes_for_table1 = pd.DataFrame([(tool_box.set_name, tool_box.session_uid,
                                               tool_box.store_id, tool_box.visit_date.isoformat()
                                               , 100, 3)], columns=['kps_name', 'session_uid', 'store_fk',
                                                                       'visit_date', 'score_1',
                                                                       'kpi_set_fk'])

        tool_box.write_to_db_result(attributes_for_table1, 'level1', tool_box.set_name)
        tool_box.commit_results_data()
        self.timer.stop('MARSRU_SANDMARSRUCalculations.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('MARSRU_PROD calculations')
#     Config.init()
#     project_name = 'marsru-sand'
# #     # session_uids = ['e85aad1b-ec24-4c2e-9eae-5cac69445f98']
# #     # session_uids = ['39150053-177f-44f8-ae66-bcbc5d4b4455']  #sand
# #     # session_uids = ['536a6ca9-92d4-48cb-89b7-0e2ece07beb9']  #prod
# #     session_uids = ['9b8c7e73-3450-4b97-9792-42374aa7ef34']
# #     session_uids = ['3d9d0649-3a21-4f3e-957d-ce13f49eae86']
#     session_uids = ['174d142e-41fe-48bf-8658-4b22adc17b7f']
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         MARSRU_SANDMARSRUCalculations(data_provider, output).run_project_calculations()

    # jg = MARSRU_SANDMARSRUJsonGenerator('marsru-sand')
    # jg.create_json('KPI MARS 12.03.17.xlsx')
    # # tb = MARSRU_SANDMARSRUKPIToolBox(data_provider, self.output, 'Hypermarket')
    # tb = MARSRU_SANDMARSRUKPIToolBox(data_provider, output, 'MARS KPIs 2017')
    # tb.insert_new_kpis_old(project_name, jg.project_kpi_dict.get('kpi_data')[0])
    # tb.insert_new_kpis_old(project_name)

