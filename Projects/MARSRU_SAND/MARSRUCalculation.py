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

        if self.data_provider.visit_date.isoformat() < '2018-01-01':
            kpi_set_name = 'MARS KPIs 2017'
            kpi_template =              ['2018/MARS KPIs.xlsx', 'kpi_data', 'KPI']  # [file name, key, sheet name]
            kpi_golden_shelves =        ['2018/MARS KPIs.xlsx', 'golden_shelves', 'golden_shelves']
            kpi_answers_translation =   ['2018/MARS KPIs.xlsx', 'survey_answers_translation', 'survey_answers_translation']
            kpi_must_range_targets =    ['2018/MARS KPIs.xlsx', 'must_range_skus', [2217, 2220, 2390, 2391, 2317, 2254]]
        else:
            kpi_set_name = 'MARS KPIs'
            kpi_template =              ['2019/MARS KPIs.xlsx', 'kpi_data', 'KPI']  # [file name, key, sheet name]
            kpi_golden_shelves =        ['2019/MARS KPIs.xlsx', 'golden_shelves', 'golden_shelves']
            kpi_answers_translation =   ['2019/MARS KPIs.xlsx', 'survey_answers_translation', 'survey_answers_translation']
            kpi_must_range_targets =    ['2019/MARS KPIs.xlsx', 'must_range_skus', [4317, 4254]]

        jg = MARSRU_SANDMARSRUJsonGenerator(project_name)
        jg.create_template_json(kpi_template[0], kpi_template[1], kpi_template[2])
        jg.create_template_json(kpi_golden_shelves[0], kpi_golden_shelves[1], kpi_golden_shelves[2])
        jg.create_template_json(kpi_answers_translation[0], kpi_answers_translation[1], kpi_answers_translation[2])
        jg.create_template_json(kpi_must_range_targets[0], kpi_must_range_targets[1], kpi_must_range_targets[2])
        kpi_templates = jg.project_kpi_dict

        tool_box = MARSRU_SANDMARSRUKPIToolBox(kpi_templates, self.data_provider, self.output, kpi_set_name)

        # tool_box.handle_update_custom_scif()

        tool_box.check_availability(kpi_templates.get('kpi_data'))
        tool_box.check_survey_answer(kpi_templates.get('kpi_data'))
        tool_box.check_number_of_scenes(kpi_templates.get('kpi_data'))
        tool_box.custom_average_shelves(kpi_templates.get('kpi_data'))
        tool_box.custom_number_bays(kpi_templates.get('kpi_data'))
        tool_box.check_price(kpi_templates.get('kpi_data'))
        tool_box.brand_blocked_in_rectangle(kpi_templates.get('kpi_data'))
        tool_box.custom_marsru_1(kpi_templates.get('kpi_data'))
        tool_box.check_layout_size(kpi_templates.get('kpi_data'))
        tool_box.golden_shelves(kpi_templates.get('kpi_data'))
        tool_box.facings_by_brand(kpi_templates.get('kpi_data'))

        # the order is important - KPI 2254 and 4254 SHOULD be calculated after linear SOS and layout size
        tool_box.must_range_skus(kpi_templates.get('kpi_data'))
        tool_box.multiple_brands_blocked_in_rectangle(kpi_templates.get('kpi_data'))
        tool_box.negative_neighbors(kpi_templates.get('kpi_data'))
        tool_box.get_total_linear(kpi_templates.get('kpi_data'))
        tool_box.get_placed_near(kpi_templates.get('kpi_data'))
        tool_box.check_availability_on_golden_shelves(kpi_templates.get('kpi_data'))
        tool_box.check_for_specific_display(kpi_templates.get('kpi_data'))

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
        'ee67f44d-5fdd-4471-82e6-36a89f7260d6',
        '00d40f25-8fdd-4941-961e-9406678aa87f',
        '6b763b32-afda-4320-ac11-95ea90fe2823'
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        data_provider.load_session_data(session)
        MARSRU_SANDMARSRUCalculations(data_provider, output).run_project_calculations()

