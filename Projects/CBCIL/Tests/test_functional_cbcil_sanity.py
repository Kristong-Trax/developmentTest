

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.CBCIL.Tests.Data.data_test_cbcil_sanity import ProjectsSanityData
from Projects.CBCIL.Calculations import CBCILCalculations
from DevloperTools.SanityTests.PsSanityTests import PsSanityTestsFuncs
from Projects.CBCIL.Tests.Data.kpi_results import CBCILKpiResults
import os
import math
import pandas as pd

__author__ = 'ilays'

PROJECT_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')

class TestKEnginePsCode(PsSanityTestsFuncs):

    def add_mocks(self):
        # with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'Relative Position.txt'),
        #           'rb') as f:
        #     relative_position_template = json.load(f)
        # self.mock_object('save_latest_templates',
        #                  path='KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil.TemplateHandler')
        # self.mock_object('download_template',
        #                  path='KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil.TemplateHandler').return_value = \In
        #     relative_position_template
        return

    def _check_obligatory_template_columns(self, sheet_data_tuple):
        """
        :param sheet_data_tuple: A tuple which contains the sheet name, the expected columns and the columns the cannot
        have nan values! (sheet_name, {col_set}, {col_set2}).
        """
        sheet_name, expected_columns, columns_without_nan = sheet_data_tuple
        template = pd.read_excel(PROJECT_TEMPLATE_PATH, sheet_name=sheet_name, skiprows=1)
        if expected_columns.difference(template.columns):
            # Gives it another shot - Maybe the redundant top row was removed
            template = pd.read_excel(PROJECT_TEMPLATE_PATH, sheet_name=sheet_name, skiprows=0)
        self.assertEqual(set(), expected_columns.difference(template.columns),
                         msg="Columns difference in the following sheet: {}".format(sheet_name))

        # Template's attributes Test
        self.assertTrue(self._check_template_instances(template, columns_without_nan),
                        msg="One of template's attributes has nan value!")

    @staticmethod
    def _check_template_instances(template, columns_to_check):
        """ This method get a DataFrame and columns that mustn't have nan values and validates it. """
        for col in columns_to_check:
            attribute_values = template[col].unique().tolist()
            for value in attribute_values:
                if isinstance(value, float) and math.isnan(value):
                    print "ERROR! There is a empty value in the following column: {}".format(col)
                    return False
        return True

    def test_current_project_template(self):
        """
        This test is check the validation of the current project's template!
        For every sheet it validates the obligatory columns + the columns that mustn't have nan value.
        Every sheet with this data represented as a tuple which contains the sheet name, the expected columns and the
        columns the cannot have nan values! (sheet_name, {col_set}, {col_set2})
        """
        sheets_data = [('KPI', {'Atomic Name', 'KPI Name', 'KPI Set', 'store_type', 'additional_attribute_1',
                                'additional_attribute_6', 'Template Name', 'Template group', 'KPI Family', 'Score Type',
                                'Param Type (1)/ Numerator',
                                'Param (1) Values', 'Param Type (2)/ Denominator', 'Param (2) Values', 'Param Type (3)',
                                'Param (3) Values', 'Weight', 'Target', 'Split Score'},
                        {'Atomic Name', 'KPI Name', 'KPI Set', 'store_type', 'additional_attribute_1',
                         'additional_attribute_6', 'Param Type (1)/ Numerator', 'KPI Family', 'Weight'}),
                       ('kpi weights', {'KPI Set', 'KPI Name', 'Weight'}, {'KPI Set', 'KPI Name', 'Weight'}),
                       ('Kpi Gap', {'KPI Name', 'Order'}, {'KPI Name', 'Order'})]
        for sheet_data in sheets_data:
            self._check_obligatory_template_columns(sheet_data)

    @PsSanityTestsFuncs.seeder.seed(["cbcil_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_cbcil_sanity(self):
        self.add_mocks()
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = {u'021262d4-f6c0-44af-adb8-02f58b17518a': []}
        kpi_results = CBCILKpiResults().get_kpi_results()
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            CBCILCalculations(data_provider, output).run_project_calculations()
            # for scene in sessions[session]:
            # data_provider.load_scene_data(str(session), scene_id=scene)
            # SceneCalculations(data_provider).calculate_kpis()
        self._assert_test_results_matches_reality(kpi_results)
        # self._assert_old_tables_kpi_results_filled()
        # self._assert_new_tables_kpi_results_filled(distinct_kpis_num=None, list_of_kpi_names=None)
        # self._assert_scene_tables_kpi_results_filled(distinct_kpis_num=None)
