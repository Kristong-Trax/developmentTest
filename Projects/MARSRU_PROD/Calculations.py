from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.MARSRU_PROD.Utils.KPIToolBox import MARSRU_PRODKPIToolBox
from Projects.MARSRU_PROD.Utils.JSONGenerator import MARSRU_PRODJSONGenerator


__author__ = 'sergey'


class MARSRU_PRODCalculations(BaseCalculationsScript):

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):
        self.timer.start()  # use log.time_message

        project_name = self.data_provider.project_name

        if self.data_provider.visit_date.isoformat() < '2019-01-01':
            # kpi_file_name = '2018/MARS KPIs.xlsx'
            # kpi_range_targets_sheet_names = [2217, 2220, 2390, 2391, 2317, 2254]
            # kpi_channels = None
            Log.error("Error: The visit date is out of date: {}. The sessions cannot be calculated."
                      "".format(self.data_provider.visit_date.isoformat()))
            return

        elif self.data_provider.visit_date.isoformat() < '2019-12-29':
            # kpi_file_name = '2019/MARS KPIs.xlsx'
            # kpi_range_targets_sheet_names = [4317, 4650, 4254]  # , 4388, 4389
            # kpi_channels = [kpi_file_name, 'channels', 'channels']
            Log.error("Error: The visit is out of date threshold [2019-12-29]: {} . The session cannot be calculated."
                      "".format(self.data_provider.visit_date.isoformat()))
            return

        elif self.data_provider.visit_date.isoformat() < '2020-06-14':
            kpi_file_name = '2020/MARS KPIs.xlsx'
            kpi_range_targets_sheet_names = [4317, 4650, 4254]
            kpi_channels = [kpi_file_name, 'channels', 'channels']

        else:
            kpi_file_name = '2020_06_14/MARS KPIs.xlsx'
            kpi_range_targets_sheet_names = [4317, 4650, 4254]
            kpi_channels = [kpi_file_name, 'channels', 'channels']

        # [file name, key, sheet name]
        kpi_template = \
            [kpi_file_name, 'kpi_data', 'KPI']
        kpi_golden_shelves = \
            [kpi_file_name, 'golden_shelves', 'golden_shelves']
        kpi_answers_translation = \
            [kpi_file_name, 'survey_answers_translation', 'survey_answers_translation']
        kpi_sku_lists = \
            [kpi_file_name, 'sku_lists', 'sku_lists']
        kpi_range_targets = \
            [kpi_file_name, 'range_targets', kpi_range_targets_sheet_names]

        jg = MARSRU_PRODJSONGenerator(project_name)

        kpis_sheet_name = None
        if kpi_channels:
            jg.create_template_json(kpi_channels[0], kpi_channels[1], kpi_channels[2])
            store_types = jg.project_kpi_dict.get('channels')
            for store_type in store_types:
                if store_type['Store type'] == self.data_provider.store_type:
                    kpis_sheet_name = store_type['KPIs_Channel']
                    break
        else:
            kpis_sheet_name = 'KPI'

        if not kpis_sheet_name:
            Log.warning("Error: Store channel is not defined for Store ID [{}] with Store type [{}]"
                        "".format(self.data_provider.store_fk, self.data_provider.store_type.encode('utf-8')))
            return

        jg.create_template_json(
            kpi_template[0], kpi_template[1], kpis_sheet_name)
        jg.create_template_json(
            kpi_golden_shelves[0], kpi_golden_shelves[1], kpi_golden_shelves[2])
        jg.create_template_json(
            kpi_answers_translation[0], kpi_answers_translation[1], kpi_answers_translation[2])
        jg.create_template_json(
            kpi_sku_lists[0], kpi_sku_lists[1], kpi_sku_lists[2])
        jg.create_template_json(
            kpi_range_targets[0], kpi_range_targets[1], kpi_range_targets[2])
        kpi_templates = jg.project_kpi_dict

        tool_box = MARSRU_PRODKPIToolBox(kpi_templates, self.data_provider, self.output)

        # Todo - Uncomment the OSA before deploying!!!
        tool_box.handle_update_custom_scif()
        tool_box.calculate_osa()
        tool_box.check_availability(kpi_templates.get('kpi_data'))
        tool_box.check_survey_answer(kpi_templates.get('kpi_data'))
        tool_box.check_number_of_scenes(kpi_templates.get('kpi_data'))
        tool_box.custom_average_shelves(kpi_templates.get('kpi_data'))
        tool_box.custom_number_bays(kpi_templates.get('kpi_data'))
        # tool_box.check_price(kpi_templates.get('kpi_data'))
        # tool_box.brand_blocked_in_rectangle(kpi_templates.get('kpi_data'))
        tool_box.custom_marsru_1(kpi_templates.get('kpi_data'))
        tool_box.check_layout_size(kpi_templates.get('kpi_data'))
        tool_box.golden_shelves(kpi_templates.get('kpi_data'))
        tool_box.facings_by_brand(kpi_templates.get('kpi_data'))
        # tool_box.multiple_brands_blocked_in_rectangle(kpi_templates.get('kpi_data'))
        # tool_box.negative_neighbors(kpi_templates.get('kpi_data'))
        tool_box.get_total_linear(kpi_templates.get('kpi_data'))
        tool_box.get_placed_near(kpi_templates.get('kpi_data'))
        tool_box.check_availability_on_golden_shelves(kpi_templates.get('kpi_data'))
        tool_box.check_for_specific_display(kpi_templates.get('kpi_data'))

        # the order is important - source KPIs must be calculated first (above)
        tool_box.check_range_kpis(kpi_templates.get('kpi_data'))
        tool_box.check_kpi_results(kpi_templates.get('kpi_data'))
        tool_box.check_block_and_neighbors_by_shelf(kpi_templates.get('kpi_data'))

        kpi_sets = tool_box.results_and_scores.keys()
        kpi_sets.remove('*')

        # Saving to old tables
        for kpi_set in kpi_sets:
            tool_box.write_to_db_result_level1(kpi_set[0])
        tool_box.commit_results_data()

        # Saving to new tables
        for kpi_set in kpi_sets:
            tool_box.store_to_new_kpi_tables_level0(kpi_set[1])
        tool_box.common.commit_results_data()

        self.timer.stop('MARSRU2_SANDProjectCalculations.run_project_calculations')
