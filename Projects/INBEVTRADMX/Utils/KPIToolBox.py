# -*- coding: utf-8 -*-

import os
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
# from Trax.Utils.Conf.Keys import DbUsers
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as Common2

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

from Projects.INBEVTRADMX.Utils import GeoLocation

__author__ = 'yoava_Jasmine_Elyashiv'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
DISPLAY_KPI = 'DISPLAY_IN_SESSION'
POSM_KPI = 'POSM_PRODUCTS'

class INBEVTRADMXToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.common2 = Common2(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.templates = self.data_provider.all_templates
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.excel_file_path = os.path.join(self.templates_path, 'inbevtradmx_template_11_v2.xlsx')
        self.availability = Availability(self.data_provider)
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.geo = GeoLocation.INBEVTRADMXGeo(self.rds_conn, self.session_uid, self.data_provider,
                                              self.kpi_static_data, self.common, self.common2)
        self.new_static_data = self.common2.kpi_static_data
        self.manufacturer_fk = 1
        self.match_displays_in_scene = self.data_provider.match_display_in_scene
        self.mpis = self.mpis_merger()
        self.all_data = pd.merge(self.scif, self.match_product_in_scene[
            ['product_fk', 'shelf_number', 'scene_fk', 'facing_sequence_number']],
                                 how="inner", left_on=['item_id', 'scene_id'],
                                 right_on=['product_fk', 'scene_fk']).drop_duplicates()

    # init functions:

    def parse_template(self):
        """
        convert excel file to data frame
        :return: data frame
        """
        template_df = pd.read_excel(self.excel_file_path, sheetname='template', encoding='utf-8')
        template_df['Store Additional Attribute 4'] = template_df['Store Additional Attribute 4'].fillna('')
        template_df['Store Additional Attribute 13'] = template_df['Store Additional Attribute 13'].fillna('')
        return template_df

    def check_store_type(self, row, relevant_columns):
        """
        this method checks if the session store type is valid
        :type relevant_columns: list
        :param row: current KPI row
        :return: boolean
        """
        # make sure that we need to check this field
        if relevant_columns.__contains__('store_type'):
            # get the valid stores from the template
            stores_df = pd.read_excel(self.excel_file_path, sheetname='store types')
            # create a list of the valid stores
            stores = stores_df.values
            return row['store_type'] in stores
        else:
            return True

    # calculation:

    def main_calculation(self):
        # calculate geo
        geo_result = self.geo.calculate_geo_location()
        self.geo.write_geo_to_db(float(geo_result))

        # calculate from template
        self.calculate_kpi_set_from_template()
        self.calculate_posm_displays()
        self.common.commit_results_data()
        self.common2.commit_results_data()

    def calculate_kpi_set_from_template(self):
        """
        this method chooses the correct set to calculate
        there will always be only on set to calculate, depending on the field 'Store additional attribute 4' from
        template
        :return: None
        """
        # get the template
        parsed_template = self.parse_template()

        # get all the unique sets
        # sets = parsed_template['KPI Level 1 Name'].unique()

        # get the session additional_attribute_4 & 13
        additional_attribute_4 = self.store_info.additional_attribute_4.values[0]
        additional_attribute_13 = self.store_info.additional_attribute_13.values[0]
        set_names = self.choose_correct_sets_to_calculate(additional_attribute_4,
                                                          additional_attribute_13, parsed_template)

        for set_name in set_names:
            # wrong value in additional attribute 4 - shouldn't calculate
            if set_name == '':
                Log.warning('Wrong value in additional attribute 4 - shouldnt calculate')
                return -1
            # get only the part of the template that is related to this set
            set_template_df = parsed_template[parsed_template['KPI Level 1 Name'] == set_name]
            # start calculating !
            self.calculate_set_score(set_template_df, set_name)

    @staticmethod
    def choose_correct_sets_to_calculate(additional_attribute_4, additional_attribute_13, template):
        """
        choose what is the appropriate set to calculate
        :param additional_attribute_4: session additional_attribute_4. if None, will ignore the kpi.
        :param additional_attribute_13: session additional_attribute_13. if None, will ignore this attribute
        :param template: parsed template
        :return: set name to calculate - assuming each additional attribute 4 matches only 1 set name.
        """
        template = template.dropna(subset=['Store Additional Attribute 4'], axis=0)
        # Makes sure attribute 4 exist
        if not additional_attribute_4:
            return ''

        if additional_attribute_13:
            sets = template[(template['Store Additional Attribute 4'].str.contains(additional_attribute_4)) &
                            ((template['Store Additional Attribute 13'].str.contains(additional_attribute_13)) |
                             (template['Store Additional Attribute 13'] == ''))]
        else:
            sets = template[(template['Store Additional Attribute 4'].str.contains(additional_attribute_4)) &
                            (template['Store Additional Attribute 13'] == '')]
        if sets.empty:
            return ''
        else:
            return sets['KPI Level 1 Name'].unique().tolist()

        # if additional_attribute_4 == 'BC':
        #     set_name = sets[0]
        # elif additional_attribute_4 == 'BA':
        #     set_name = sets[1]
        # elif additional_attribute_4 == 'MODELORAMA':
        #     set_name = sets[2]
        # else:
        #     return ''
        # return set_name

    def calculate_set_score(self, set_df, set_name):
        """
        this method iterates kpi set and calculates it's score
        :param set_df: the set df to calculate score for
        :param set_name: the kpi set name
        :return: None
        """
        set_score = 0
        # get array of all kpi level 2 names
        kpi_names = set_df['KPI Level 2 Name'].unique()
        # iterate all kpi level 2
        for kpi_name in kpi_names:
            # calculate kpi level 2 score
            kpi_score = self.calculate_kpi_level_2_score(kpi_name, set_df, set_name)
            # accumulate set score
            set_score += kpi_score
        # round set_score if the set is 'Set Urban'
        if set_name == 'Set Urban':
            set_score = round(set_score)
        # finally, write level 1 kpi set score to DB
        self.write_kpi_set_score_to_db(set_name, set_score)

    def calculate_kpi_level_2_score(self, kpi_name, set_df, set_name):
        """
        this method gets kpi level 2 name, and iterates it's related atomic kpis
        :param set_name: kpi set name
        :param kpi_name: kpi level 2 name
        :param set_df: kpi set df
        :return: kpi level 2 score
        """
        kpi_df = set_df[set_df['KPI Level 2 Name'].str.encode('utf8') == kpi_name.encode('utf-8')]
        kpi_score = 0
        # iterate the all related atomic kpis
        for i, row in kpi_df.iterrows():
            # get atomic kpi name
            kpi_level_3_name = row['KPI Level 3 Name']
            # calculate atomic kpi score
            atomic_kpi_score = self.calculate_atomic_kpi_score(row, kpi_level_3_name, kpi_name, set_name)
            # accumulate kpi level 2 score
            kpi_score += atomic_kpi_score
        write_to_all_levels = False
        if len(kpi_df) > 1:  # if there is just one atomic we don't need two levels
            write_to_all_levels = True
        elif kpi_df['KPI Level 3 Name'].iloc[0] != kpi_df['KPI Level 2 Name'].iloc[0]:
            write_to_all_levels = True
        self.write_kpi_score_to_db(kpi_name, set_name, kpi_score, write_to_all_levels)
        return kpi_score

    def calculate_atomic_kpi_score(self, row, kpi_level_3_name, kpi_name, set_name):
        """
        this method calculates score for specific atomic kpi
        :param set_name: kpi set name
        :param kpi_name: kpi name
        :param kpi_level_3_name: the atomic kpi name
        :param row: atomic kpi details
        :return: atomic kpi score
        """
        atomic_kpi_score = 0
        # get column name to consider in calculation
        relevant_columns = map(str.strip, str(row['column names']).split(','))
        optional_columns = map(str.strip, str(row['optional column']).split(','))
        is_kpi_passed = 0
        if self.check_store_type(row, relevant_columns):
            # get weight of current atomic kpi
            curr_weight = row['weights']
            # figure out what type of calculation need to be done
            if row['KPI type'] == 'Product Availability':
                if kpi_level_3_name == 'URBAN':
                    score = self.calculate_weigthed_availability_score(row, relevant_columns)
                    if score:
                        atomic_kpi_score = score
                elif kpi_level_3_name == 'Hay o no hay # frentes':
                    if self.calculate_lead_availability_score(row, relevant_columns):
                        is_kpi_passed = 1
                elif kpi_level_3_name == 'Adherencia de materiales':
                    if self.calculate_or_availability(row, relevant_columns, optional_columns):
                        is_kpi_passed = 1
                else:
                    if self.calculate_availability_score(row, relevant_columns):
                        is_kpi_passed = 1
            elif row['KPI type'] == 'SOS':

                ratio = self.calculate_sos_score(row, relevant_columns)
                if (row['product_type'] == 'Empty') & (ratio <= 0.2):
                    if self.scif[self.scif['template_name'] == row['template_name']].empty:
                        is_kpi_passed = 0
                    else:
                        is_kpi_passed = 1
                elif ratio == 1:
                    is_kpi_passed = 1
            elif row['KPI type'] == 'Survey':
                if self.calculate_survey_score(row):
                    is_kpi_passed = 1
            if is_kpi_passed == 1:
                atomic_kpi_score += curr_weight
            # write result to DB
            # the customer asked for this specific KPI will write 100 in DB if it passed even if the weight is 0
            if kpi_level_3_name == 'Sin Espacios Vacios' and curr_weight == 0 and is_kpi_passed == 1:
                # atomic_kpi_score = 100
                self.write_atomic_to_db(kpi_level_3_name, 100, kpi_name, set_name, is_kpi_passed, curr_weight)
            else:
                self.write_atomic_to_db(kpi_level_3_name, atomic_kpi_score, kpi_name, set_name, is_kpi_passed,
                                        curr_weight)
        return atomic_kpi_score

    # sos

    def calculate_sos_score(self, row, relevant_columns):
        """
        this method calculates share of shelf score according to columns from the data frame
        :param relevant_columns: columns to check in the excel file
        :param row: data frame to calculate from
        :return: share of shelf score
        """

        if 'shelf_number' in relevant_columns:
            scif_index_list = []
            df = self.all_data[(self.all_data.template_name == row['template_name']) &
                               (self.all_data.shelf_number == row['shelf_number'])&
                               (self.all_data.facing_sequence_number > 0)]
            df.shelf_number = df.shelf_number.astype(float)
            df.shelf_number = df.shelf_number.astype(str)
            df = df[df['product_type'] != 'Empty']

        else:
            # get df only with the correct template name
            df = self.scif[self.scif.template_name == row['template_name']]
        if not pd.isna(row['exclude product_type']):
            for excl in [e.strip() for e in row['exclude product_type'].split(',')]:
                df = df[df['product_type'] != excl]

        # sum of all the facings in df
        facings = df.facings.values.sum()
        if facings == 0:
            return 0
        # create dictionary for calculating
        filters_dict = self.create_sos_filtered_dictionary(relevant_columns, row)
        # reduce the df only to relevant columns
        df = df[filters_dict.keys()]


        # check if it's invasion KPI for the special case
        inv = row['KPI Level 3 Name'][:3] == 'Inv'
        ratio = self.calculate_sos_ratio(df, filters_dict, inv)



        return ratio / facings

    def calculate_sos_ratio(self, df, filters_dict, inv):
        ratio = 0

        if 'manufacturer_name' in df.columns.tolist():
            filter_out =  df[(df['product_type'] ==('Empty')) & (df['manufacturer_name'] == 'General')].index
            df.drop(filter_out, inplace=True)

        for key in filters_dict:
            delete = [key for value in filters_dict[key] if value in ['nan']]
        for key in delete:
            del filters_dict[key]

        # iterate the data frame
        for i, df_row in df.iterrows():
            # initialize the boolean variable
            bol = True
            # special case that the customer asked
            # if inv and df_row.product_type == 'Empty':
            if df_row.product_type == 'Empty':
                ratio = ratio + self.scif.facings.loc[i]
                continue
            # iterate the filtered dictionary keys
            else:
                for key in filters_dict.keys():
                    # check if the value in df row in in "key" column is in the filtered dictionary we created before
                    bol &= df_row[key] in filters_dict[key]
            # that means success of the inner loop, that all the values matching for this data frame row
            if bol:
                if 'shelf_number' in filters_dict.keys():
                    ratio = ratio + self.all_data.facings.loc[i]
                else:
                    # accumulate ratio
                    ratio = ratio + self.scif.facings.loc[i]
        return ratio

    def create_sos_filtered_dictionary(self, relevant_columns, row):
        """
        this method filters out relevant columns from the template fso we can calculate SOS easily
        :param relevant_columns: relevant columns from temolates
        :param row: specific row from template
        :return: dictionary
        """
        # dictionary to send to the generic method
        filters_dict = {}
        self.handle_exclude_skus(filters_dict, relevant_columns, row)
        # fill the dictionary
        for column_value in relevant_columns:
            if column_value in ['Store Additional Attribute 4', 'Store Additional Attribute 13'] or column_value == 'store_type':
                continue
            filters_dict[column_value] = map(str.strip, str(row.loc[column_value]).split(','))
        return filters_dict

    # availability:

    def calculate_weigthed_availability_score(self, row, relevant_columns):
        """
        this method calculates availability score according to columns from the data frame
        :param row: data frame to calculate from
        :param relevant_columns: columns to check in the excel file
        :return: boolean
        """
        passed = 0
        # Gets the scene types
        scene_types = row['template_name'].split(', ')
        scenes_num = len(scene_types)
        # man = row['manufacturer_name']
        if 'scene_type' in relevant_columns:
            relevant_columns.remove('scene_type')
        # create filtered dictionary
        filters_dict = self.create_availability_filtered_dictionary(relevant_columns, row)
        for scene in scene_types:
            filters_dict.update({'template_name': scene})
            # call the generic method from KPIUtils_v2
            if self.availability.calculate_availability(**filters_dict):
                passed += 1
        return (passed / float(scenes_num)) * 100 if scenes_num else 0

    def calculate_lead_availability_score(self, row, relevant_columns):
        """
        this method calculates availability score according to columns from the data frame
        :param row: data frame to calculate from
        :param relevant_columns: columns to check in the excel file
        :return: boolean
        """
        # Gets the brand names
        brand_names = row['brand_name'].split(', ')
        if 'scene_type' in relevant_columns:
            relevant_columns.remove('scene_type')

        # create filtered dictionary
        filters_dict = self.create_availability_filtered_dictionary(relevant_columns, row)
        for brand in brand_names:
            filters_dict.update({'brand_name': brand})
            # call the generic method from KPIUtils_v2
            availability_score = self.availability.calculate_availability(**filters_dict)
            if self.decide_availability_score(row, availability_score):
                return True
        return False

    def calculate_availability_score(self, row, relevant_columns):
        """
        this method calculates availability score according to columns from the data frame
        :param row: data frame to calculate from
        :param relevant_columns: columns to check in the excel file
        :return: boolean
        """
        # create filtered dictionary
        filters_dict = self.create_availability_filtered_dictionary(relevant_columns, row)
        for key in filters_dict:
            delete = [key for value in filters_dict[key] if value in ['nan']]
        for key in delete:
            del filters_dict[key]
        # call the generic method from KPIUtils_v2
        availability_score = self.availability.calculate_availability(**filters_dict)
        # check if this score should pass or fail
        return self.decide_availability_score(row, availability_score)

    def create_availability_filtered_dictionary(self, relevant_columns, row):
        """
        this method creates a dictionary with keys according to the specific row in the template
        :param relevant_columns: columns to create keys by
        :param row: the specific row in the template
        :return: dictionary
        """
        # dictionary to send to the generic method
        filters_dict = {}
        self.handle_exclude_skus(filters_dict, relevant_columns, row)
        # fill the dictionary
        for column_value in relevant_columns:
            if column_value == 'Store Additional Attribute 4' or column_value == 'store_type' or \
                    column_value == 'Store Additional Attribute 13':
                continue
            filters_dict[column_value] = map(str.strip, str(row.loc[column_value]).split(','))
        return filters_dict


    def calculate_or_availability(self, row, relevant_columns, optional_columns):
        """
                this method calculates availability score according to columns from the data frame
                :param row: data frame to calculate from
                :param relevant_columns: columns to check in the excel file
                :return: boolean
                """
        for optional_column in optional_columns:
            # create filtered dictionary
            temp_relevant_columns = relevant_columns[:]
            temp_relevant_columns.append(optional_column)
            filters_dict = self.create_availability_filtered_dictionary(temp_relevant_columns, row)
            for key in filters_dict:
                delete = [key for value in filters_dict[key] if value in ['nan']]
            for key in delete:
                del filters_dict[key]
            # call the generic method from KPIUtils_v2
            availability_score = self.availability.calculate_availability(**filters_dict)
            # check if this score should pass or fail
            if self.decide_availability_score(row, availability_score):
                return True


        return False

    def filter_product_names(self, exclude_skus):
        """
        this method filters list of SKUs from self.scif
        :param exclude_skus:  list of SKUs to exclude from template
        :return: filtered list
        """
        return filter(lambda sku: sku not in exclude_skus, self.scif.product_name.values)

    @staticmethod
    def decide_availability_score(row, availability_score):
        """
        this method decides if the score should pass or fail according to the template
        :param row: scpecific row from template
        :param availability_score: score
        :return: Boolean
        """
        if availability_score == 0:
            return False
        else:
            if row['KPI Level 1 Name'] == 'Set Modeloramas' and row['KPI Level 3 Name'] == 'Hay o no hay Pop?':
                if row['KPI Level 2 Name'] == 'Pop Exterior':
                    return availability_score > 1
                elif row['KPI Level 2 Name'] == 'Pop Interior':
                    return availability_score > 1
            elif row['KPI Level 1 Name'] == 'Set Self Execution' and row[
                'KPI Level 3 Name'] == 'Hay o no hay # frentes':
                return availability_score > 24
            else:
                return True

    # surveys:

    def calculate_survey_score(self, row):
        """
        this method calculates survey score according to columns from the data frame
        :param row: data frame to calculate from
        :return: boolean
        """
        question_code = str(int(row['Survey Question Code']))
        if not self.survey_response.empty and \
                not self.survey_response[self.survey_response.code == question_code].empty:
            answer = self.survey_response.selected_option_text[self.survey_response.code == question_code].values[0]
            if answer == 'Si':
                return True
            else:
                if row['KPI Level 2 Name'] == 'Primer Impacto' and answer == 'No tiene Enfirador':
                    return True
            return False
        else:
            return False

    # help functions:

    def calculate_posm_displays(self):
        new_kpi_set_fk = self.common2.get_kpi_fk_by_kpi_name(DISPLAY_KPI)
        for display_fk in self.match_displays_in_scene['display_fk'].to_list():
            self.common2.write_to_db_result(fk=new_kpi_set_fk, result=0,
                                            numerator_id=self.manufacturer_fk, denominator_id=display_fk)

        new_kpi_set_fk = self.common2.get_kpi_fk_by_kpi_name(POSM_KPI)
        for product_fk in self.scif['product_fk'][self.scif['location_type'] == 'POSM'].to_list():
            self.common2.write_to_db_result(fk=new_kpi_set_fk, result=0,
                                            numerator_id=self.manufacturer_fk, denominator_id=product_fk)



    def handle_exclude_skus(self, filters_dict, relevant_columns, row):
        """
        this method checks if there is value in 'exclude skus' column in the template.
        if exists, it filters out the relevant skus from the calculation
        :param filters_dict: filtered dictionary
        :param relevant_columns: columns to create keys by
        :param row: specific row to calculate
        :return: None
        """
        try:
            exclude_skus = row['exclude skus'].split(',')
        except AttributeError:
            exclude_skus = []
        if exclude_skus:
            # filter out some product names according to template
            product_names = self.filter_product_names(exclude_skus)
            filters_dict['product_name'] = product_names
        if 'exclude skus' in row.to_dict().keys() and 'exclude skus' in relevant_columns:
            relevant_columns.remove('exclude skus')

    # db functions:

    def write_kpi_set_score_to_db(self, set_name, set_score):
        """
        this method writes set kpi score to static.kps_results DB
        :param set_name: set name
        :param set_score: set score
        :return: None
        """
        # kpi_set_fk = self.kpi_static_data.kpi_set_fk[self.kpi_static_data.kpi_set_name == set_name].unique()[0]
        # self.common.write_to_db_result(kpi_set_fk, self.LEVEL1, set_score)
        new_kpi_set_fk = self.common2.get_kpi_fk_by_kpi_name(set_name)
        self.common2.write_to_db_result(fk=new_kpi_set_fk, result=set_score,
                                        numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                                        identifier_result=self.common2.get_dictionary(name=set_name))

    def write_kpi_score_to_db(self, kpi_name, set_name, kpi_score, write_to_all_levels):
        """
        this method writes kpi score to static.kpk_results DB
        :param kpi_name: name of level 2 kpi
        :param set_name: name of related set
        :param kpi_score: the score
        :return: None
        """
        # kpi_fk = \
        #     self.kpi_static_data.kpi_fk[
        #         (self.kpi_static_data.kpi_name.str.encode('utf-8') == kpi_name.encode('utf-8')) &
        #         (self.kpi_static_data.kpi_set_name == set_name)].values[0]
        # self.common.write_to_db_result(kpi_fk, self.LEVEL2, kpi_score)
        if write_to_all_levels:
            new_kpi_fk = self.common2.get_kpi_fk_by_kpi_name(kpi_name)
            self.common2.write_to_db_result(fk=new_kpi_fk, result=kpi_score, should_enter=True,
                                            identifier_parent=self.common2.get_dictionary(name=set_name),
                                            identifier_result=self.common2.get_dictionary(name=kpi_name))

    def write_atomic_to_db(self, atomic_name, atomic_score, kpi_name, set_name, is_kpi_passed, curr_weight):
        """
        this method writes atomic kpi score to static.kpi_results DB
        :param curr_weight: current weight of atomic kpi
        :param is_kpi_passed: is this kpi passed
        :param atomic_name: atomic kpi name
        :param atomic_score: the score
        :param kpi_name: name of related kpi
        :param set_name: name of related set
        :return:
        """
        # atomic_kpi_fk = \
        #     self.kpi_static_data.atomic_kpi_fk[(self.kpi_static_data.atomic_kpi_name == atomic_name) &
        #                                        (self.kpi_static_data.kpi_name == kpi_name) &
        #                                        (self.kpi_static_data.kpi_set_name == set_name)].values[0]
        # attrs = self.common.create_attributes_dict(fk=atomic_kpi_fk, score=is_kpi_passed, level=self.LEVEL3)
        # attrs['result'] = {0: atomic_score}
        # attrs['kpi_weight'] = {0: curr_weight}
        # query = insert(attrs, self.common.KPI_RESULT)
        # self.common.kpi_results_queries.append(query)
        identifier_parent = self.common2.get_dictionary(name=kpi_name)
        if atomic_name == kpi_name:
            identifier_parent = self.common2.get_dictionary(name=set_name)

        new_atomic_fk = self.common2.get_kpi_fk_by_kpi_name(atomic_name)

        # kpi_fk = \
        # self.new_static_data[self.new_static_data['client_name'].str.encode('utf-8') == kpi_name.encode('utf-8')][
        #     'pk'].values[0]
        self.common2.write_to_db_result(
            fk=new_atomic_fk, result=atomic_score, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
            weight=curr_weight, should_enter=True, score=is_kpi_passed,
            identifier_parent=identifier_parent)

    def mpis_merger(self):
        try:
            mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
            .merge(self.templates, on='template_fk', suffixes=['', '_t'])
            return mpis
        except:
            pass