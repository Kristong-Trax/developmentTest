import os

from Trax.Data.Testing.Seed import Seeder
from Trax.Utils.Testing.Case import TestCase, MockingTestCase
from mock import patch, MagicMock
import pandas as pd

from Projects.GOOGLEUS_SAND.Utils.KPIToolBox import GOOGLEUS_SANDToolBox


class TestMarsuk(MockingTestCase):

    @property
    def import_path(self):
        return 'Projects.GOOGLEUS_SAND.Utils.KPIToolBox'

    def setUp(self):
        super(TestMarsuk, self).setUp()
        self._mock_data_provider()
        self._mock_project_connector()
        self.mock_object('DbUsers')
        self.mock_get_display_matches()
        self.general_tool_box_mock = self.mock_object('GOOGLEUS_SANDGENERALToolBox')
        check_availability_mock = self.general_tool_box_mock.return_value.calculate_availability
        check_availability_mock.return_value = 5
        self.mock_object('GOOGLEUS_SANDToolBox.get_price_data')
        self.mock_get_template()

    def _mock_data_provider(self):
        self._data_provider = MagicMock()
        self._data_provider_data = {}

        def get_item(key):
            return self._data_provider_data[key] if key in self._data_provider_data else MagicMock()
        self._data_provider.__getitem__.side_effect = get_item

    def mock_get_display_matches(self):
        display_mock = self.mock_object('GOOGLEUS_SANDToolBox.get_match_display')
        display_mock.return_value = pd.DataFrame({})

    def mock_get_template(self):
        display_mock = self.mock_object('GOOGLEUS_SANDToolBox.get_custom_template')
        records = []
        display_mock.return_value = pd.DataFrame.from_records(records)

    def _mock_store_type(self, store_type):
        self._data_provider_data['store_info'] = pd.DataFrame({'store_type': [store_type]})

    def _mock_project_connector(self):
        self.mock_object('ProjectConnector')

    def _mock_scene_item_facts(self, data):
        self._data_provider_data['scene_item_facts'] = data

    def test_availability(self):
        tool_box = GOOGLEUS_SANDToolBox(MagicMock(), MagicMock())
        result = tool_box.calculate_availability('kpi_test', 'test_tab', 'test_set')
        expected = 'x'
        self.assertEqual(result, expected)