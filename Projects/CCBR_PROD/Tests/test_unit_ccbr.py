import pandas as pd
from Trax.Utils.Testing.Case import TestUnitCase
from Tests.TestUtils import remove_cache_and_storage

__author__ = 'avrahama'


class TestCCCBR(TestUnitCase):

    @property
    def import_path(self):
        return 'Projects.CCBR_PROD.Utils.KPIToolBox.CCBRToolBox'

    def set_up(self):
        super(TestCCCBR, self).set_up()
        remove_cache_and_storage()

    def test_test(self):
        pass