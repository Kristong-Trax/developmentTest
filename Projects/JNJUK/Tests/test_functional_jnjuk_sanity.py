
import os
import MySQLdb
import math
import pandas as pd
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Projects.JNJUK.Tests.Data.test_data_jnjuk_sanity import ProjectsSanityData
from Projects.JNJUK.Calculations import JNJUKCalculations

from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Tests.TestUtils import remove_cache_and_storage


__author__ = 'ilays'

PROJECT_DATA_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')


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
    
    @seeder.seed(["jnjuk_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_jnjuk_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = {'89a8d548-d747-4567-844a-7b2b3252331d': []}
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            JNJUKCalculations(data_provider, output).run_project_calculations()
            # self._assert_old_tables_kpi_results_filled()
            self._assert_new_tables_kpi_results_filled()
            # for scene in sessions[session]:
            #     data_provider.load_scene_data(str(session), scene_id=scene)
            #     SceneCalculations(data_provider).calculate_kpis()
            #     self._assert_scene_tables_kpi_results_filled()

    def test_project_template(self):
        """
        This test is responsible to validate the project's template. It validate existance and that there aren't
        any empty values (unless it is allowed).
        """
        required_templates = ['eye_level_jnjuk.xlsx', 'SurveyTemplate.xlsx', 'KPI Exclusions Template.xlsx']
        allowed_empty_values_template = ['KPI Exclusions Template.xlsx']
        current_templates_in_project = os.listdir(PROJECT_DATA_PATH)
        for template in required_templates:
            self.assertIn(template, current_templates_in_project,
                          msg="The following template is missing: {}".format(template))
            if template in allowed_empty_values_template:
                continue
            error_col, values_validation = self._check_template_instances(template)
            self.assertTrue(values_validation, msg="There a missing value in the template {} in the following "
                                                   "column: {}".format(template, error_col))

    def _check_template_instances(self, template_name):
        """
        This test going over all of the columns of the template and makes
        sure that it doesn't have empty values.
        """
        template_df = self._read_template(template_name)
        for col in template_df.columns:
            attribute_values = template_df[col].unique().tolist()
            for value in attribute_values:
                if isinstance(value, float) and math.isnan(value):
                    print "ERROR! There is a empty value in the following column: {}".format(col)
                    return col, False
        return "OK", True

    @staticmethod
    def _read_template(template_name):
        template_path = os.path.join(PROJECT_DATA_PATH, template_name)
        template = pd.read_excel(template_path, skiprows=0)
        if 'Unnamed' in template.columns[0]:  # Give it another shot, we allow empty first row
            template = pd.read_excel(template_path, skiprows=1)
        return template
