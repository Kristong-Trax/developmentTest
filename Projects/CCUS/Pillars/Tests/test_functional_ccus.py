import os
import random
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Data.Testing.SeedNew import Seeder
from Projects.CCUS.Pillars.Tests.Data.test_data_ccus import ProjectsSanityData
from Projects.CCUS.Pillars.Utils.KPIToolBox import PillarsPROGRAMSToolBox
from Projects.DIAGEOUS.Utils.Const import Const
from Tests.TestUtils import remove_cache_and_storage
from KPIUtils_v2.DB.CommonV2 import Common


__author__ = 'avrahama'


class TestCcus(TestFunctionalCase):
    seeder = Seeder()

    def set_up(self):
        super(TestCcus, self).set_up()
        remove_cache_and_storage()
        self.project_name = ProjectsSanityData.project_name
        self.session_uid = '8395fc95-465b-47c2-ad65-6d10de13cd75'

    @property
    def import_path(self):
        return 'Projects.CCUS.Pillars.Utils.KPIToolBox'

    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')

    # functional tests - testing the output of some of the methods

    @seeder.seed(["mongodb_products_and_brands_seed", "ccus_seed"], ProjectsSanityData())
    def test_round_result_outpute(self):
        output = Output()
        data_provider = KEngineDataProvider(self.project_name)
        data_provider.load_session_data(self.session_uid)
        commonv2 = Common(data_provider)
        self.tool_box = PillarsPROGRAMSToolBox(data_provider, output, commonv2)
