import pandas as pd

from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
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
        jg.create_json('MARS KPIs 2018.xlsx', year_filter='2018')
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
        tool_box.check_availability_on_golden_shelves(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.check_for_specific_display(jg.project_kpi_dict.get('kpi_data')[0])
        attributes_for_table1 = pd.DataFrame([(tool_box.set_name,
                                               tool_box.session_uid,
                                               tool_box.store_id,
                                               tool_box.visit_date.isoformat(),
                                               100,
                                               3)],
                                             columns=['kps_name',
                                                      'session_uid',
                                                      'store_fk',
                                                      'visit_date',
                                                      'score_1',
                                                      'kpi_set_fk'])

        tool_box.write_to_db_result(attributes_for_table1, 'level1', tool_box.set_name)
        tool_box.commit_results_data()
        self.timer.stop('MARSRU_PRODMARSRUCalculations.run_project_calculations')

if __name__ == '__main__':
    LoggerInitializer.init('MARSRU_PROD calculations')
    Config.init()
    project_name = 'marsru-prod'
    session_uids = [
        '4675635c-1e40-41a2-b11e-6472382df1b0',
        'ea0f81e1-eea6-45c9-880f-7a2581a081e7',
        '905f5087-8fca-41c3-a901-f9a82e684137',
        '3ffaf3e2-de72-4001-af7e-397062b3bd6f'
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        data_provider.load_session_data(session)
        MARSRU_PRODMARSRUCalculations(data_provider, output).run_project_calculations()

    # jg = MARSRU_PRODMARSRUJsonGenerator('marsru-prod')
    # jg.create_json('KPI MARS 16.03.17.xlsx')
    # # tb = MARSRU_PRODMARSRUKPIToolBox(data_provider, self.output, 'Hypermarket')
    # tb = MARSRU_PRODMARSRUKPIToolBox(data_provider, output, 'MARS KPIs 2017')
    # tb.insert_new_kpis_old(project_name, jg.project_kpi_dict.get('kpi_data')[0])
    # tb.insert_new_kpis_old(project_name)
