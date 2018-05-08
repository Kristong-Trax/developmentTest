import os

from Trax.Data.Testing.Seed import Seeder
from Trax.Utils.Testing.Case import TestCase
from mock import patch, MagicMock
import pandas as pd
from Projects.RIPETCAREUK_PROD.Calculation import MarsUkCalculations
from Projects.RIPETCAREUK_PROD.Tests.TestWriter import TestKpiResultsWriter
from Projects.RIPETCAREUK_PROD.Tests.data_test_unit_marsuk import DataTestUnitMarsUk
from Projects.RIPETCAREUK_PROD.Utils.ParseTemplates import KPIConsts


class TestMarsuk(TestCase):

    seeder = Seeder()
    mock_path = 'Projects.RIPETCAREUK_PROD.Utils.KPIToolBox'

    def setUp(self):
        super(TestCase, self).setUp()
        self._mock_data_provider()
        self.patchers = []
        self.mock_template_path()
        self._mock_project_connector()
        self.mock_static_kpi()
        self.mock_writer_kpi()

    def _mock_data_provider(self):
        self._data_provider = MagicMock()
        self._data_provider_data = {}

        def get_item(key):
            return self._data_provider_data[key] if key in self._data_provider_data else MagicMock()
        self._data_provider.__getitem__.side_effect = get_item

    def mock_static_kpi(self):

        static_patch = patch('{}.get_all_kpi_static_data'.format(self.mock_path))
        static_mock = static_patch.start()
        static_mock.return_value = DataTestUnitMarsUk.static_data
        self.patchers.append(static_patch)

    def mock_writer_kpi(self):

        patcher = patch('{}.MarsUkPerfectScore._get_writer'.format(self.mock_path))
        self._writer_mock = patcher.start()
        writer = TestKpiResultsWriter()
        self._writer_mock.return_value = writer
        self.patchers.append(patcher)

    def _mock_store_type(self, store_type):
        self._data_provider_data['store_info'] = pd.DataFrame({'store_type': [store_type]})

    def _mock_template(self, template_data):
        template_patcher = patch('{}.ParseMarsUkTemplates'.format(self.mock_path))
        template_mock = template_patcher.start()
        template_mock.return_value.parse_templates.return_value = template_data
        self.patchers.append(template_patcher)

    def _mock_project_connector(self):
        patcher = patch('{}.AwsProjectConnector'.format(self.mock_path))
        patcher.start()
        self.patchers.append(patcher)

    def _mock_scene_item_facts(self, data):
        self._data_provider_data['scene_item_facts'] = data

    def tearDown(self):
        super(TestCase, self).tearDown()
        for patcher in self.patchers:
            patcher.stop()

    def mock_template_path(self):
        template_patcher = patch('Projects.RIPETCAREUK_PROD.Utils.ParseTemplates.TEMPLATE_NAME',
                                 os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Test_Template.xlsx'))
        template_patcher.start()
        self.patchers.append(template_patcher)

    @staticmethod
    def _get_kpk_results():
        pass

    @staticmethod
    def _get_kpi_results():
        pass

    @staticmethod
    def _get_kps_results():
        pass

    def test_nun_of_facings_atomic_kpis_calculated_correctly_above_threshold(self):

        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.num_of_facings_atomic_kpi_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003_4_facings))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[results['atomic_kpi_name'] == 'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,']
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 4)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 100)

    def test_nun_of_facings_atomic_kpis_calculated_correctly_below_threshold(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.num_of_facings_atomic_kpi_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[results['atomic_kpi_name'] == 'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,']
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 2)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 0)
        sheba_fresh_choice = results[results['atomic_kpi_name'] == 'SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,']
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 0)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 0)

    def test_nun_of_facings_atomic_kpis_calculated_correctly_with_scene_type_filter(self):

        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_kpi_data_with_scene_type_filter,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.num_of_facings_atomic_kpi_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(pd.DataFrame.from_records(
            DataTestUnitMarsUk.scene_item_facts_product_4770608247003_4_facings +
            DataTestUnitMarsUk.scene_item_facts_product_4770608247003_different_scene_type))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[results['atomic_kpi_name'] == 'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,']
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 4)

    def test_kpi_binary_0_when_one_of_atomic_is_0(self):

        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.assortment_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'] == 'Sheba')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 0)

    def test_kpi_binary_1_when_both_of_atomic_is_1(self):

        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.assortment_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003+
                                                              DataTestUnitMarsUk.scene_item_facts_product_3065890099692))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'] == 'Sheba')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 100)

    def test_kpi_accumulative_bottom_group_only(self):

        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.assortment_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_5900951112935))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'] == 'Whi Pouch')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 50)

    def test_kpi_accumulative_top_group_only(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.assortment_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_5900951113758))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'] == 'Whi Pouch')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 0)

    def test_kpi_accumulative_all_groups(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.assortment_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_5900951112935+
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951113758))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'] == 'Whi Pouch')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 100)

    def test_kpi_accumulative_bottom_group_multiple_atomic_in_group(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.accumulative_scoring_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.accumulative_scoring_atomic_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003 +
                                      DataTestUnitMarsUk.scene_item_facts_product_3065890099692 +
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951112935))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'] == 'Whi Pouch')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 50)

    def test_kpi_proportional_one_passed_out_of_two(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.proportional_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.assortment_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'] == 'Sheba')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 50)

    def test_kpi_proportional_all_pass(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.proportional_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.assortment_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003 +
                                      DataTestUnitMarsUk.scene_item_facts_product_3065890099692))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'] == 'Sheba')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 100)

    def test_kpi_proportional_none_pass(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.proportional_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.assortment_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_5900951113758))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'] == 'Sheba')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 0)

    def test_kpi_proportional_groups(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.proportional_group_kpi_data,
            KPIConsts.FACINGS_SHEET: DataTestUnitMarsUk.proportional_group_atomic_kpi_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003 +
                                      DataTestUnitMarsUk.scene_item_facts_product_3065890099692 +
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951112935))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'] == 'Sheba')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 62.5)

    def test_pillar_score_sum_of_weighted_scores(self):

        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.assortment_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003+
                                                              DataTestUnitMarsUk.scene_item_facts_product_3065890099692+
                                                              DataTestUnitMarsUk.scene_item_facts_product_5900951112935))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'].isnull()) &
                                     (results['set_name'] == 'ASSORTMENT SCORE')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 6)
        sheba_fresh_choice = results[(results['kpi_name'] == 'ASSORTMENT SCORE') &
                                     (results['set_name'] == 'PERFECT STORE')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 6)

    def test_kpi_excluded_from_calculation_are_excluded_proportional(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.proportional_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.atomic_kpi_data_for_excluded
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_3065890099692))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'] == 'Sheba')]
        self.assertEquals(len(sheba_fresh_choice.index), 1)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 100)

    def test_kpi_has_not_result_if_atomic_has_no_score(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_and_facings_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.atomic_kpi_data_for_excluded,
            KPIConsts.FACINGS_SHEET: DataTestUnitMarsUk.facings_atomic_kpi_data_with_test_store_type
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_3065890099692))
        self._mock_store_type('Hypermarket_test')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results

        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'] == 'Sheba')&
                                     (results['set_name'] == 'ASSORTMENT SCORE')]
        self.assertEquals(len(sheba_fresh_choice.index), 0)

    def test_pillar_has_not_result_if_kpis_has_no_score(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_and_facings_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.atomic_kpi_data_for_excluded,
            KPIConsts.FACINGS_SHEET: DataTestUnitMarsUk.facings_atomic_kpi_data_with_test_store_type
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_3065890099692))
        self._mock_store_type('Hypermarket_test')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results

        sheba_fresh_choice = results[(results['atomic_kpi_name'].isnull()) & (results['kpi_name'].isnull()) &
                                     (results['set_name'] == 'ASSORTMENT SCORE')]
        self.assertEquals(len(sheba_fresh_choice.index), 0)

        sheba_fresh_choice = results[(results['kpi_name'] == 'ASSORTMENT SCORE') &
                                     (results['set_name'] == 'PERFECT STORE')]
        self.assertEquals(len(sheba_fresh_choice.index), 0)

    def test_perfect_store_has_not_result_if_pillars_has_no_score(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_kpi_data,
            KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.atomic_kpi_data_for_excluded,
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_3065890099692))
        self._mock_store_type('Hypermarket_test')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results

        self.assertEquals(len(results.index), 0)

    def test_atomic_sos_brand_and_sub_category_calculated_correctly_above_threshold(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.sos_kpi_data,
            KPIConsts.SOS_SHEET: DataTestUnitMarsUk.sos_atomic_kpi_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003 +
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951112935 +
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951113758))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'] == 'Sheba') &
                                     (results['set_name'] == 'Share of Shelf SCORE')]
        self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 0.5)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 100)

    def test_atomic_sos_brand_and_sub_category_calculated_correctly_below_threshold(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.sos_kpi_data,
            KPIConsts.SOS_SHEET: DataTestUnitMarsUk.sos_atomic_kpi_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003 +
                                      DataTestUnitMarsUk.scene_item_facts_product_3065890099692 +
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951112935 +
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951113758))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'] == 'Sheba') &
                                     (results['set_name'] == 'Share of Shelf SCORE')]
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 0)

    def test_atomic_sos_sub_category_calculated_correctly_above_threshold(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.sos_kpi_data,
            KPIConsts.SOS_SHEET: DataTestUnitMarsUk.sos_atomic_kpi_sub_category_only_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003 +
                                      DataTestUnitMarsUk.scene_item_facts_product_3065890099692 +
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951112935 +
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951113758))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'] == 'Sheba') &
                                     (results['set_name'] == 'Share of Shelf SCORE')]
        self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 0.75)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 100)

    def test_atomic_sos_sub_category_calculated_correctly_above_threshold_with_scene_type_filter(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.sos_kpi_data_with_scene_type,
            KPIConsts.SOS_SHEET: DataTestUnitMarsUk.sos_atomic_kpi_sub_category_only_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003_different_scene_type +
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951112935 +
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951113758))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'] == 'Sheba') &
                                     (results['set_name'] == 'Share of Shelf SCORE')]
        self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 0.5)

    def test_atomic_sos_sub_category_calculated_correctly_below_threshold(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.sos_kpi_data,
            KPIConsts.SOS_SHEET: DataTestUnitMarsUk.sos_atomic_kpi_sub_category_only_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003 +
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951113758))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'] == 'Sheba') &
                                     (results['set_name'] == 'Share of Shelf SCORE')]
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 100)

    def test_atomic_clip_strips_correctly_above_threshold(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.clip_strip_kpi_data,
            KPIConsts.CLIP_STRIP_SHEET: DataTestUnitMarsUk.clip_strip_atomic_kpi_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003_ce_dreamies))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'] == 'Dreamies') &
                                     (results['set_name'] == 'Clip strips Score')]
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 100)

    def test_atomic_clip_strips_consider_only_when_both_product_type_and_brand_below_threshold(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.clip_strip_kpi_data,
            KPIConsts.CLIP_STRIP_SHEET: DataTestUnitMarsUk.clip_strip_atomic_kpi_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_3065890099692_ce_not_dreamies +
                                      DataTestUnitMarsUk.scene_item_facts_product_5900951112935_dreamies_not_ce))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'] == 'Dreamies') &
                                     (results['set_name'] == 'Clip strips Score')]
        self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 0)
        self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 0)

    def test_atomic_clip_strips_filter_by_scene_types(self):
        template_data = {
            KPIConsts.SHEET_NAME: DataTestUnitMarsUk.clip_strip_kpi_data_with_scene_type_filter,
            KPIConsts.CLIP_STRIP_SHEET: DataTestUnitMarsUk.clip_strip_atomic_kpi_data
        }
        self._mock_template(template_data)
        self._mock_scene_item_facts(
            pd.DataFrame.from_records(DataTestUnitMarsUk.scene_item_facts_product_4770608247003_ce_dreamies +
                                      DataTestUnitMarsUk.scene_item_facts_product_4770608247003_ce_dreamies_wrong_scene_type))
        self._mock_store_type('Hypermarket')
        output = MagicMock()
        MarsUkCalculations(self._data_provider, output).run_project_calculations()
        results = self._writer_mock.return_value._kpi_results
        sheba_fresh_choice = results[(results['atomic_kpi_name'] == 'Dreamies') &
                                     (results['set_name'] == 'Clip strips Score')]
        self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 2)






