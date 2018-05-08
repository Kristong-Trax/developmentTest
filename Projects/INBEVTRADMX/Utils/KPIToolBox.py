# coding=utf-8

import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
# from Trax.Utils.Conf.Keys import DbUsers
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

from Projects.INBEVTRADMX.Utils import GeoLocation

__author__ = 'yoava'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class INBEVTRADMXToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.excel_file_path = os.path.join(self.templates_path, 'TEMPLATE V2.3 KPIs TRAD.xlsx')
        self.availability = Availability(self.data_provider)
        self.sos = SOS(self.data_provider, self.output)
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.survey = Survey(self.data_provider, self.output)
        self.geo = GeoLocation.Geo(self.rds_conn, self.session_uid, self.data_provider, self.kpi_static_data)

    def parse_template(self):
        """
        convert excel file to data frame
        :return: data frame
        """
        template_df = pd.read_excel(self.excel_file_path, sheetname='template')
        return template_df

    def filter_product_names(self, exclude_skus):
        """
        this method filters list of SKUs from self.scif
        :param exclude_skus:  list of SKUs to exclude from template
        :return: filtered list
        """
        return filter(lambda sku: sku not in exclude_skus, self.scif.product_name.values)

    def calculate_availability_score(self, row):
        """
        this method calculates availability score according to columns from the data frame
        :param row: data frame to calculate from
        :return: boolean
        """
        # get column name to consider in calculation
        relevant_columns = map(str.strip, str(row['column names']).split(','))
        # dictionary to sed to the generic method
        filters_dict = {}
        exclude_skus = row['exclude skus'].split(',')
        if not exclude_skus:
            product_names = self.filter_product_names(exclude_skus)
            filters_dict['product_name'] = product_names
        relevant_columns.remove('exclude skus')
        # fill the dictionary
        for column_value in relevant_columns:
            if column_value == 'scene_type':
                filters_dict['template_name'] = row.loc[column_value].split(',')
            else:
                filters_dict[column_value] = row.loc[column_value].split(',')
        # call the generic method from KPIUtils_v2
        if self.availability.calculate_availability(**filters_dict) > 0:
            return True
        else:
            return False

    def calculate_sos_score(self, row):
        """
        this method calculates share of shelf score according to columns from the data frame
        :param row: data frame to calculate from
        :return: share of shelf score
        """
        # get column name to consider in calculation
        relevant_columns = map(str.strip, str(row['column names']).split(','))
        # dictionary to sed to the generic method
        filters_dict = {}
        # fill the dictionary
        for column_value in relevant_columns:
            filters_dict[column_value] = row.loc[column_value].split(',')
        # call the generic method from KPIUtils_v2
        return self.sos.calculate_share_of_shelf(filters_dict)

    def calculate_survey_score(self, row):
        """
        this method calculates survey score according to columns from the data frame
        :param row: data frame to calculate from
        :return: boolean
        """
        survey_question = row['survey question']
        survey_answers = row['survey answers'].split(',')
        # call the generic method from KPIUtils_v2
        answer = self.survey.get_survey_answer(survey_question)
        # call the generic method from KPIUtils_v2
        return self.survey.check_survey_answer(survey_question, answer) in survey_answers

    def calculate_set_score(self, set_df):
        """
        this method iterates each row from the data frame and accumulates set score according to the conditions
        :param set_df: data frame for this set
        :return: set score
        """
        total_score = 0
        for i, row in set_df.iterrows():
            curr_weight = row['weights']
            if row['KPI type'] == 'Product Availability':
                if self.calculate_availability_score(row):
                    total_score += curr_weight
            elif row['KPI type'] == 'SOS':
                ratio = self.calculate_sos_score(row)
                if (row['product_type'] == 'Empty') & (ratio <= 0.2):
                    total_score += curr_weight
                elif ratio >= 0.98:
                    total_score += curr_weight
            elif row['KPI type'] == 'Survey':
                if self.calculate_survey_score(row):
                    total_score += curr_weight
        return total_score

    def check_store_type(self, df):
        """
        this method checks if the session store type is valud
        :param df: the set data frame
        :return: boolean
        """
        stores = set()
        for store_types in df['store types']:
            types = store_types.split(",")
            for store_type in types:
                stores.add(store_type)
        return self.data_provider.store_type in stores

    def calculate_kpis_from_template(self, score):
        parsed_template = self.parse_template()
        sets = parsed_template['KPI Level 1 Name'].unique()
        for set_name in sets:
            set_template_df = parsed_template[parsed_template['KPI Level 1 Name'] == set_name]
            if self.check_store_type(set_template_df):
                score = self.calculate_set_score(set_template_df)
            else:
                Log.error('store type not valid')
        return score

    def main_calculation(self):
        # calculate geo
        geo_result = self.geo.calculate_geo_location()
        self.geo.write_geo_to_db(float(geo_result))

        # calculate from template
        # score = self.calculate_kpis_from_template(score)
