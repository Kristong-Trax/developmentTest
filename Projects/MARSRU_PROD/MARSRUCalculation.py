import pandas as pd

from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.MARSRU_PROD.Utils.MARSRUToolBox import MARSRU_PRODMARSRUKPIToolBox
from Projects.MARSRU_PROD.Utils.MARSRUJSON import MARSRU_PRODMARSRUJsonGenerator

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
# from KPIUtils.Utils.Helpers.LogHandler import log_handler

__author__ = 'urid'


class MARSRU_PRODMARSRUCalculations(BaseCalculationsScript):

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):
        self.timer.start()  # use log.time_message
        tool_box = MARSRU_PRODMARSRUKPIToolBox(self.data_provider, self.output, 'MARS KPIs 2017')
        tool_box.handle_update_custom_scif()
        jg = MARSRU_PRODMARSRUJsonGenerator('marsru-prod')
        jg.create_json('MARS KPIs.xlsx', year_filter='2018')
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
                    '6b763b32-afda-4320-ac11-95ea90fe2823',
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
