import os

import MySQLdb

from Trax.Utils.Testing.Case import MockingTestCase

from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase

# from Projects.RIPETCAREUK_PROD.Calculation import MarsUkCalculations
# from Projects.DIAGEOZA.Calculations import DIAGEOZACalculations as diageoza_calc
# from Projects.DIAGEOGTR.Calculations import DIAGEOGTRDIAGEOGTRCalculations as diageogtr_calc
# from Projects.DIAGEOGTR.Utils.ToolBox import DIAGEOGTRDIAGEOGTRDIAGEOToolBox as Toolbox
# from Projects.DIAGEOUK.Calculations import DIAGEOUKCalculations as diageouk_calc
# from Projects.DIAGEOTW.Calculations import DIAGEOTWCalculations as diageotw_calc
# from Projects.DIAGEOAR.Calculations import DIAGEOARDIAGEOARCalculations as diageoar_calc
# from Projects.DIAGEOAU.Calculations import DIAGEOAUCalculations as diageoau_calc
# from Projects.DIAGEOBENELUX.Calculations import DIAGEOBENELUXCalculations as diageobenelux_calc
# from Projects.DIAGEOBR.Calculations import DIAGEOBRCalculations as diageobr_calc
# from Projects.DIAGEOGA.Calculations import DIAGEOGADIAGEOGACalculations as diageoga_calc
# from Projects.DIAGEOIE.Calculations import DIAGEOIECalculations as diageoie_calc
# from Projects.DIAGEOGR.Calculations import DIAGEOGRCalculations as diageogr_calc
# from Projects.DIAGEOMX.Calculations import DIAGEOMXCalculations as diageomx_calc
# from Projects.DIAGEOPT.Calculations import DIAGEOPTCalculations as diageopt_calc
# from Projects.DIAGEOKE.Calculations import DIAGEOKECalculations as diageoke_calc
# from Projects.PNGJP.Calculations import PNGJPCalculations as pngcalc
from Tests.TestUtils import remove_cache_and_storage

__author__ = 'idanr'


class TestKEngineOutOfTheBox(TestFunctionalCase):
    
    def set_up(self):
        super(TestKEngineOutOfTheBox, self).set_up()
        remove_cache_and_storage()
    
    @property
    def import_path(self):
        return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'

    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')

    seeder = Seeder()

    def _assert_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("""
        SELECT * FROM report.kpi_results
        """)
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    def _assert_table_row_count(self, table, row_count):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("""SELECT * FROM {table}""".format(table=table))
        self.assertEquals(cursor.rowcount, row_count)

    # @seeder.seed(["ccru_seed"], ProjectsSanityData())
    # def test_ccru_sanity(self):
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     sessions = ['8DD169D2-EFE1-4B5F-8DA7-A805EADA17B7']
    #     for session in sessions:
    #         data_provider.load_session_data(session)
    #         output = Output()
    #         ccru_calc(data_provider, output).run_project_calculations()
    #         self._assert_kpi_results_filled()

    # @seeder.seed(["ccza_seed"], ProjectsSanityData())
    # def test_ccza_sanity(self):
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     sessions = ['2F32EB97-FE41-4E7C-A501-386C5BC8BDAC', '067E07F0-1B23-435D-8D0A-FCC85505CC73']
    #     for session in sessions:
    #         data_provider.load_session_data(session)
    #         output = Output()
    #         ccza_calc(data_provider, output).run_project_calculations()
    #         self._assert_kpi_results_filled()
    #
    # @seeder.seed(["inbevtradmx_seed"], ProjectsSanityData())
    # def test_inbevtradmx_sanity(self):
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     sessions = ['3fe0c096-3bd2-4a50-a91b-e1bd47b7ee43']
    #     for session in sessions:
    #         data_provider.load_session_data(session)
    #         output = Output()
    #         inbevtradmxcalc(data_provider, output).run_project_calculations()
    #         self._assert_kpi_results_filled()
    #
    #
    # @patch('Projects.CCUS.Utils.ToolBox.ToolBox.get_latest_directory_date_from_cloud', return_value='2018-01-01')
    # @patch('Projects.CCUS.Utils.ToolBox.ToolBox.save_latest_templates')
    # @patch('Projects.CCUS.Utils.ToolBox.ToolBox.download_template', return_value=dunkin_donuts_json)
    # @seeder.seed(["ccus_seed"], ProjectsSanityData())
    # def test_ccus_sanity(self,x ,y , json):
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     sessions = ['e8d584d1-c14c-4bec-818a-739ffebcbc40', 'F87F022D-0B9B-4BC6-AE2F-AFD83558BD3C']
    #     for session in sessions:
    #         data_provider.load_session_data(session)
    #         output = Output()
    #         ccus_calc(data_provider, output).run_project_calculations()
    #         self._assert_kpi_results_filled()
    #
    # # NOT PASSING
    # @seeder.seed(["marsru_seed"], ProjectsSanityData())
    # def test_marsru_sanity(self):
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     sessions = ['41d83b2f-e610-47db-be37-4c2049a3b0c4', '3f41cbe7-323c-4858-9d08-b60857e9204d']
    #     for session in sessions:
    #         data_provider.load_session_data(session)
    #         output = Output()
    #         marsru_calc(data_provider, output).run_project_calculations()
    #         self._assert_kpi_results_filled()

    # # NOT PASSING
    # @seeder.seed(["pngamerica_seed"], ProjectsSanityData())
    # def test_pngamerica_sanity(self):
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     sessions = ['6beac915-eda9-4308-afa8-f568b0f6dad0', '513254d2-b88b-4d8a-9665-dce5b61b0136']
    #     for session in sessions:
    #         data_provider.load_session_data(session)
    #         output = Output()
    #         pngamerica_calc(data_provider, output).run_project_calculations()
    #         self._assert_kpi_results_filled()

    # # NOT PASSING
    # @seeder.seed(["ccbottlersus_seed"], ProjectsSanityData())
    # def test_ccbottlersus_sanity(self):
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     sessions = ['38C26BEE-B754-49DE-B455-B5D958599B1E', '1858C3D1-F2FB-4981-BD24-94FAA5E092D7']
    #     for session in sessions:
    #         data_provider.load_session_data(session)
    #         output = Output()
    #         ccbottlersus_calc(data_provider, output).run_project_calculations()
    #         self._assert_kpi_results_filled()

    # # NOT PASSING
    # @seeder.seed(["diageoza_seed"],
    #              ProjectsSanityData())
    # def test_diageoza_sanity(self):
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     sessions = ['1A39D55C-FAA3-4997-8001-0FBDF11BC4C3', 'ad5c5f05-2c4c-4e13-9a95-13efd9ddd115']
    #     for session in sessions:
    #         data_provider.load_session_data(session)
    #         output = Output()
    #         diageoza_calc(data_provider, output).run_project_calculations()
    #         self._assert_kpi_results_filled()

    # # NOT PASSING
    # @patch('Projects.DIAGEOGTR.Utils.ToolBox.DIAGEOGTRDIAGEOGTRDIAGEOToolBox.get_latest_directory_date_from_cloud',
    #        return_value='2018-01-01')
    # @patch('Projects.DIAGEOGTR.Utils.ToolBox.DIAGEOGTRDIAGEOGTRDIAGEOToolBox.save_latest_templates')
    # @patch('Projects.DIAGEOGTR.Utils.ToolBox.DIAGEOGTRDIAGEOGTRDIAGEOToolBox.download_template', return_value='json')
    # @seeder.seed(["diageogtr_seed"], ProjectsSanityData())
    # def test_diageogtr_sanity(self, get_cloud, y, z):
    #     # s3 = StorageFactory.get_connector('bucket', region=AwsRegions.NORTH_VIRGINIA)
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     sessions = ['FC3C5771-5C08-4C90-94E8-D23F510F147C']
    #     for session in sessions:
    #         data_provider.load_session_data(session)
    #         output = Output()
    #         diageogtr_calc(data_provider, output).run_project_calculations()
    #         self._assert_kpi_results_filled()

    # # NOT PASSING
    # @seeder.seed(["diageouk_seed"], ProjectsSanityData())
    # def test_diageouk_sanity(self):
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     sessions = ['67BB0B7D-2202-49B0-91F7-95973F85CABF', '857C66FD-C588-4001-AD42-B49398616983']
    #     for session in sessions:
    #         data_provider.load_session_data(session)
    #         output = Output()
    #         diageouk_calc(data_provider, output).run_project_calculations()
    #         self._assert_kpi_results_filled()

    # # NOT PASSING
    # @seeder.seed(["diageotw_seed"], ProjectsSanityData())
    # def test_diageotw_sanity(self):
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     sessions = ['0FF7F783-C610-4504-B219-F1CA27F0D16B', '85A4625D-9DEF-4E33-89AC-C4819DEBD5DC']
    #     for session in sessions:
    #         data_provider.load_session_data(session)
    #         output = Output()
    #         diageotw_calc(data_provider, output).run_project_calculations()
    #         self._assert_kpi_results_filled()

    # # NOT PASSING
    # @seeder.seed(["ccbottlersus_seed"], ProjectsSanityData())
    # def test_ccbottlersus_sanity(self):
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     session = '85897dc0-17f8-4c02-9cc6-596729e1ec11'
    #     data_provider.load_session_data(session)
    #     output = Output()
    #     CCBOTTLERSCalculations(data_provider, output).run_project_calculations()
    #     self._assert_kpi_results_filled()


if __name__ == '__main__':
    tests = TestKEngineOutOfTheBox()
    tests.test_ccru_sanity()
    # tests.test_ccza_sanity()
    # tests.test_inbevtradmx_sanity()
    # tests.test_ccus_sanity()
    # tests.test_marsru_sanity()

