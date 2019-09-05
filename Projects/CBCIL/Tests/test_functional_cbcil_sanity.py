
import os
import math
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
import MySQLdb
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
import pandas as pd
from Projects.CBCIL.Tests.Data.test_data_cbcil_sanity import ProjectsSanityData
from Projects.CBCIL.Calculations import CBCILCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase

from Tests.TestUtils import remove_cache_and_storage

__author__ = 'yoava'

PROJECT_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


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
    
    def _assert_old_tables_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
           SELECT * FROM report.kpi_results
           ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    def _assert_new_tables_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
           SELECT * FROM report.kpi_level_2_results
           ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    def _assert_scene_tables_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
           SELECT * FROM report.scene_kpi_results
           ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()
    
    @seeder.seed(["cbcil_seed"], ProjectsSanityData())
    def test_cbcil_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['f7acade6-7e4f-4617-8012-7857bd7ffbc9']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            CBCILCalculations(data_provider, output).run_project_calculations()
            self._assert_old_tables_kpi_results_filled()
            self._assert_new_tables_kpi_results_filled()

    def test_current_project_template(self):
        """ This test is check the validation of the current project's template! """
        # Columns tests
        expected_columns = {'Atomic Name', 'KPI Name', 'KPI Set', 'store_type', 'additional_attribute_1',
                            'Template Name', 'Template group', 'KPI Family', 'Score Type', 'Param Type (1)/ Numerator',
                            'Param (1) Values', 'Param Type (2)/ Denominator', 'Param (2) Values', 'Param Type (3)',
                            'Param (3) Values', 'Weight', 'Target', 'Split Score'}
        template = pd.read_excel(PROJECT_TEMPLATE_PATH, skiprows=1)
        if expected_columns.difference(template.columns):
            # Gives it another shot - Maybe the redundant top row was removed
            template = pd.read_excel(PROJECT_TEMPLATE_PATH, skiprows=0)
        self.assertEqual(set(), expected_columns.difference(template.columns),
                         msg="The template's columns are different than the expected ones!")

        # Template's attributes Test
        self.assertTrue(self._check_template_instances(template), msg="One of template's attributes has nan value!")

    @staticmethod
    def _check_template_instances(template):
        columns_to_check = ['Atomic Name', 'KPI Name', 'KPI Set', 'store_type', 'additional_attribute_1',
                            'Param Type (1)/ Numerator', 'KPI Family', 'Weight']
        for col in columns_to_check:
            attribute_values = template[col].unique().tolist()
            for value in attribute_values:
                if isinstance(value, float) and math.isnan(value):
                    print "ERROR! There is a empty value in the following column: {}".format(col)
                    return False
        return True
