import pandas as pd

from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.MARSRU_SAND.Utils.MARSRUToolBox import MARSRU_SANDMARSRUKPIToolBox
from Projects.MARSRU_SAND.Utils.MARSRUJSON import MARSRU_SANDMARSRUJsonGenerator

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'urid'


class MARSRU_SANDMARSRUCalculations(BaseCalculationsScript):

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):
        self.timer.start()  # use log.time_message

        if self.data_provider.visit_date.isoformat() < '2019-01-01':
            kpi_set_name = 'MARS KPIs 2017'
            kpi_template =              '2018\\MARS KPIs.xlsx'
            kpi_golden_shelves =        '2018\\golden_shelves.xlsx'
            kpi_answers_translation =   '2018\\answers_translation.xlsx'
            kpi_must_range_targets =    '2018\\must_range_targets.xlsx'
            kpi_must_range_list = [2217, 2220, 2390, 2391, 2317, 2254]
        else:
            kpi_set_name = 'MARS KPIs'
            kpi_template =              '2018\\MARS KPIs.xlsx'
            kpi_golden_shelves =        '2018\\golden_shelves.xlsx'
            kpi_answers_translation =   '2018\\answers_translation.xlsx'
            kpi_must_range_targets =    '2018\\must_range_targets.xlsx'
            kpi_must_range_list = [4317, 4254]

        jg = MARSRU_SANDMARSRUJsonGenerator(project_name)
        jg.create_template_json(kpi_template)
        jg.create_targets_json(kpi_golden_shelves, 'golden_shelves')
        jg.create_targets_json(kpi_answers_translation, 'survey_answers_translation')
        jg.create_targets_json(kpi_must_range_targets, 'must_range_skus', kpi_must_range_list)

        tool_box = MARSRU_SANDMARSRUKPIToolBox(self.data_provider, self.output, kpi_set_name)

        # tool_box.handle_update_custom_scif()

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

        # Saving to old tables
        tool_box.write_to_db_result_level1()
        tool_box.commit_results_data()

        # Saving to new tables
        tool_box.store_to_new_kpi_tables_level0(kpi_set_name)
        tool_box.common.commit_results_data()

        self.timer.stop('MARSRU_SANDMARSRUCalculations.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('MARSRU calculations')
    Config.init()
    project_name = 'marsru-sand'
    session_uids = [
        # 'e930601d-8c70-4d80-96d6-aec50966cbfe',
        'ee67f44d-5fdd-4471-82e6-36a89f7260d6',
        '00d40f25-8fdd-4941-961e-9406678aa87f',
        '6b763b32-afda-4320-ac11-95ea90fe2823'
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        data_provider.load_session_data(session)
        MARSRU_SANDMARSRUCalculations(data_provider, output).run_project_calculations()

