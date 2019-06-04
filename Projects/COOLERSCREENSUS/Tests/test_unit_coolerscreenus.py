import pandas as pd

from Trax.Utils.Testing.Case import MockingTestCase
from mock import MagicMock
from Projects.COOLERSCREENSUS.Utils.KPIGenerator import COOLERSCREENSUSKGenerator

__author__ = 'israels'


class Test_COOLERSCREENSUS(MockingTestCase):
    @property
    def import_path(self):
        return 'Projects.COOLERSCREENSUS.Utils.KPIGenerator'

    def set_up(self):
        super(Test_COOLERSCREENSUS, self).set_up()
        self.mock_data_provider()
        self.kpi_results = []
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.mock_object('DbUsers', path='KPIUtils_v2.DB.CommonV2'), self.mock_object('DbUsers')
        self.common = self.mock_object('Common', path='KPIUtils.DB.Common')
        self.output = MagicMock()
        self.mock_object('PSProjectConnector', value=MagicMock(), path='KPIUtils_v2.DB.PsProjectConnector')
        self.mock_object(object_name='Common.write_to_db_result_new_tables',
                         value=self._write_to_db, path='KPIUtils.DB.Common')
        self.mock_object('_find_prev_product', value=self._find_prev_product,
                         path='Projects.COOLERSCREENSUS.Utils.KPIGenerator.COOLERSCREENSUSKGenerator')

    def _write_to_db(self, fk, numerator_id, numerator_result, result, denominator_result=0, score=0):
        row = {'fk': fk, 'numerator_id': numerator_id, 'numerator_result': numerator_result,
               'result': result, 'denominator_result': denominator_result, 'score': score}
        self.kpi_results.append(row)

    @staticmethod
    def _find_prev_product(self, b, empty_matches):
        return empty_matches['prev_product']

    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item

    def test_same_prev_products(self):
        empty_matches = (pd.DataFrame(columns=['product_fk', 'bay_number', 'shelf_number', 'facing_sequence_number', 'creation_time', 'prev_product', 'shelf_number_from_bottom'],
                                      data=[[1, 1, 1, 1, '2019-01-01', 1578, 1], [1, 1, 2, 4, '2019-01-01', 1578, 2]]))
        COOLERSCREENSUSKGenerator(self.data_provider_mock, self.output, self.common).find_prev_products_and_write_to_db(empty_matches)
        self.assertTrue(len(self.kpi_results) == 2)

    def test_different_prev_products(self):
        empty_matches = (pd.DataFrame(columns=['product_fk', 'bay_number', 'shelf_number', 'facing_sequence_number', 'creation_time', 'prev_product', 'shelf_number_from_bottom'],
                                      data=[[1, 1, 1, 1, '2019-01-01', 1578, 1], [1, 1, 2, 4, '2019-01-01', 1579, 2]]))
        COOLERSCREENSUSKGenerator(self.data_provider_mock, self.output, self.common).find_prev_products_and_write_to_db(empty_matches)
        self.assertTrue(len(self.kpi_results) == 2)

    def test_none_prev_products(self):
        empty_matches = (pd.DataFrame(columns=['product_fk', 'bay_number', 'shelf_number', 'facing_sequence_number', 'creation_time', 'prev_product', 'shelf_number_from_bottom'],
                                      data=[[1, 1, 1, 1, '2019-01-01', None, 1], [1, 1, 2, 4, '2019-01-01', None, 2]]))
        COOLERSCREENSUSKGenerator(self.data_provider_mock, self.output, self.common).find_prev_products_and_write_to_db(empty_matches)
        self.assertTrue(len(self.kpi_results) == 0)
