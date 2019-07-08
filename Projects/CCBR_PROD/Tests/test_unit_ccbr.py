import pandas as pd
from Trax.Utils.Testing.Case import TestUnitCase, MagicMock
from Tests.TestUtils import remove_cache_and_storage
from Projects.CCBR_PROD.Utils.KPIToolBox import CCBRToolBox

__author__ = 'avrahama'


class TestCCCBR(TestUnitCase):

    @property
    def import_path(self):
        return 'Projects.CCBR_PROD.Utils.KPIToolBox.CCBRToolBox'

    def set_up(self):
        super(TestCCCBR, self).set_up()
        remove_cache_and_storage()

        # mock PSProjectConnector
        self.ProjectConnector_mock = self.mock_object(
            'Common', path='KPIUtils.DB.Common')
        self.ProjectConnector_mock = self.mock_object(
            'PSProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.ProjectConnector_mock = self.mock_object(
            'PsDataProvider', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')

        # mock 'data provider' and object
        self.data_provider_mock = MagicMock()
        self.output_mock = MagicMock()

        self.tool_box = CCBRToolBox(self.data_provider_mock, self.output_mock)


    def test_test(self):

        pass