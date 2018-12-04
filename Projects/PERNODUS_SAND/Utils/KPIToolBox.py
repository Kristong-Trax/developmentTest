import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
from KPIUtils_v2.Calculations.BlockCalculations  import Block
from KPIUtils_v2.Calculations.EyeLevelCalculations import EYE_LEVEL_DEFINITION
from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from Projects.PERNODUS_SAND.Utils.ParseTemplates import parse_template
# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
import KPIUtils_v2.Calculations.EyeLevelCalculations as EL
from Projects.PERNODUS_SAND.Utils.Const import Const

__author__ = 'nicolaske'



CATEGORIES = []
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', "Pernod_US KPI_PS v0.1.xlsx")

class PERNODUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    EXCLUDE_EMPTY = True
    INCLUDE_FILTER = 1
    DEFAULT = 'Default'
    TOP = 'Top'
    BOTTOM = 'Bottom'
    STRICT_MODE = ALL = 1000



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
        self.Presence_template = parse_template(TEMPLATE_PATH, "Presence")
        self.BaseMeasure_template = parse_template(TEMPLATE_PATH, "Base Measurement")
        self.Anchor_template = parse_template(TEMPLATE_PATH, "Anchor")
        self.Blocking_template = parse_template(TEMPLATE_PATH, "Blocking")
        self.Adjaceny_template = parse_template(TEMPLATE_PATH, "Adjacency")
        self.Eye_Level_template = parse_template(TEMPLATE_PATH, "Eye Level")
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.availability = Availability(self.data_provider)
        self.blocking_calc = Block(self.data_provider)

    def main_calculation(self, *args, **kwargs):


        #Base Measurement
        for i, row in self.BaseMeasure_template.iterrows():
            try:
                    kpi_name = row['KPI']
                    value = row ['value']
                    location = row['Store Location']

                    self.calculate_category_space(135, kpi_name, value, location)

            except Exception as e:
                Log.info('KPI {} calculation failed due to {}'.format(kpi_name.encode('utf-8'), e))
                continue




        # Anchor
        for i, row in self.Anchor_template.iterrows():
            try:
                    kpi_name = row['KPI']
                    value = row ['value']


                    self.calculate_anchor(kpi_name)

            except Exception as e:
                Log.info('KPI {} calculation failed due to {}'.format(kpi_name.encode('utf-8'), e))
                continue


        #Presence
        self.calculate_presence()

        #Blocking
        for i, row in self.Blocking_template.iterrows():
            try:
                    kpi_name = row['KPI']
                    self.calculate_blocking(kpi_name)

            except Exception as e:
                Log.info('KPI {} calculation failed due to {}'.format(kpi_name.encode('utf-8'), e))
                continue

        #Eye Level
        for i, row in self.Eye_Level_template.iterrows():
            try:
                kpi_name = row['KPI']
                self.calculate_eye_level(kpi_name)

            except Exception as e:
                Log.info('KPI {} calculation failed due to {}'.format(kpi_name.encode('utf-8'), e))
                continue


        self.common.commit_results_data()

        return

    def get_templates(self):

        for sheet in Const.SHEETS_MAIN:
            self.templates[sheet] = pd.read_excel(Const.TEMPLATE_PATH, sheetname=sheet,
                                                  keep_default_na=False)

    def calculate_blocking(self, kpi_name):
        template = self.Blocking_template.loc[self.Blocking_template['KPI'] == kpi_name]
        kpi_template = template.loc[template['KPI'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]


        relevant_filter = {kpi_template['param']:kpi_template['value']}

        result = self.blocking_calc.network_x_block_together(relevant_filter, location={'template_name':'Shelf'}, additional={'minimum_facing_for_block':2})

        score = 0
        if result.empty:
            pass
        else:
            score = 1

        if kpi_template['param'] == "brand_name":
            brand_fk = self.scif['brand_fk'][self.scif["brand_name"] == kpi_template['value']]
            brand_fk = brand_fk.iloc[0]
            self.common.write_to_db_result(fk=141, numerator_id=brand_fk, denominator_id=999,
                                           result=score, score=score)

        if kpi_template['param'] == "sub_category":
            sub_category_fk = self.scif["sub_category_fk"][self.scif["sub_category"] == kpi_template['value']]
            sub_category_fk = sub_category_fk.iloc[0]
            self.common.write_to_db_result(fk=142, numerator_id=sub_category_fk, denominator_id=999,
                                           result=score, score=score)

        if kpi_template['param'] == "size":
            self.common.write_to_db_result(fk=143, numerator_id=999, numerator_result = 375,
                                           denominator_id=999,
                                           result=score, score=score)




    def calculate_presence(self):
        """
        Use facings to determine presence of specific UPCs, brands, or segments - INBEVBE
        """
        for i, row in self.Presence_template.iterrows():


            param_type = row[Const.PARAM_TYPE]
            param_values = str(row[Const.PARAM_VALUES]).replace(', ', ',').split(',')
            general_filters = {}
            general_filters[param_type] = param_values

            filtered_df = self.scif[self.get_filter_condition(self.scif, **general_filters)]

            if row['list']:
                template_fk = filtered_df['template_fk'].iloc[0]
                brand_fks= filtered_df['brand_fk'].unique().tolist()
                for brand_fk in brand_fks:
                    self.common.write_to_db_result(fk=139, numerator_id=brand_fk, denominator_id=template_fk,
                                                   result=1, score=1)

            else:
                result = len(filtered_df[param_type].unique())
                if result == len(param_values):
                    score = 1
                else:
                    score = 0

                if param_type == 'sub_brand':
                    brand_fk = filtered_df['brand_fk'].iloc[0]
                    self.common.write_to_db_result(fk=136, numerator_id=brand_fk, denominator_id=999,
                                               result=score, score=score)
                elif param_type == 'template_name':
                    template_fk = filtered_df['template_fk'].iloc[0]
                    self.common.write_to_db_result(fk=137, numerator_id=template_fk, denominator_id=999,
                                                   result=1, score=1)
        return

    def calculate_adjacency(self):
        score = result = threshold = 0

        params = self.adjacency_data[self.adjacency_data['fixed KPI name'] == 'kpi_variable']

        group_a = {self.PRODUCT_EAN_CODE_FIELD: self._get_ean_codes_by_product_group_id(
            'Product Group Id;A', **params)}
        group_b = {self.PRODUCT_EAN_CODE_FIELD: self._get_ean_codes_by_product_group_id(
            'Product Group Id;B', **params)}

        allowed_filter = self._get_allowed_products(
            {'product_type': ['Irrelevant', 'Empty', 'Other']})
        allowed_filter_without_other = self._get_allowed_products(
            {'product_type': ['Irrelevant', 'Empty']})
        scene_filters = {'template_name': ''} #Get the templte names


        # list = calculate_adjacency_list()
        pass


    def calculate_adjacency_list(self, filter_group_a, filter_group_b, scene_type_filter, allowed_filter,
                            allowed_filter_without_other, a_target, b_target, target):
        a_product_list = self.toolbox._get_group_product_list(filter_group_a)
        b_product_list = self.toolbox._get_group_product_list(filter_group_b)

        adjacency = self._check_groups_adjacency(a_product_list, b_product_list, scene_type_filter, allowed_filter,
                                                 allowed_filter_without_other, a_target, b_target, target)

    def _check_groups_adjacency(self, a_product_list, b_product_list, scene_type_filter, allowed_filter,
                                allowed_filter_without_other, a_target, b_target, target):
        a_b_union = list(set(a_product_list) | set(b_product_list))

        a_filter = {'product_fk': a_product_list}
        b_filter = {'product_fk': b_product_list}
        a_b_filter = {'product_fk': a_b_union}
        a_b_filter.update(scene_type_filter)

        matches = self.data_provider.matches
        relevant_scenes = matches[self.toolbox.get_filter_condition(matches, **a_b_filter)][
            'scene_fk'].unique().tolist()

        result = False
        for scene in relevant_scenes:
            a_filter_for_block = a_filter.copy()
            a_filter_for_block.update({'scene_fk': scene})
            b_filter_for_block = b_filter.copy()
            b_filter_for_block.update({'scene_fk': scene})
            try:
                a_products = self.toolbox.get_products_by_filters('product_fk', **a_filter_for_block)
                b_products = self.toolbox.get_products_by_filters('product_fk', **b_filter_for_block)
                if sorted(a_products.tolist()) == sorted(b_products.tolist()):
                    return False
            except:
                pass
            if a_target:
                brand_a_blocked = self.block.calculate_block_together(allowed_products_filters=allowed_filter,
                                                                      minimum_block_ratio=a_target,
                                                                      vertical=False, **a_filter_for_block)
                if not brand_a_blocked:
                    continue

            if b_target:
                brand_b_blocked = self.block.calculate_block_together(allowed_products_filters=allowed_filter,
                                                                      minimum_block_ratio=b_target,
                                                                      vertical=False, **b_filter_for_block)
                if not brand_b_blocked:
                    continue

            a_b_filter_for_block = a_b_filter.copy()
            a_b_filter_for_block.update({'scene_fk': scene})

            block = self.block.calculate_block_together(allowed_products_filters=allowed_filter_without_other,
                                                        minimum_block_ratio=target, block_of_blocks=True,
                                                        block_products1=a_filter, block_products2=b_filter,
                                                        **a_b_filter_for_block)
            if block:
                return True
        return result

    def calculate_category_space(self, kpi_set_fk, kpi_name, category, scene_types=None):
        template = self.BaseMeasure_template.loc[(self.BaseMeasure_template['KPI'] == kpi_name) &
                                              (self.BaseMeasure_template['Store Location'] == scene_types)]
        kpi_template = template.loc[template['KPI'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        values_to_check = []

        if kpi_template['param']:
            values_to_check = str(kpi_template['value']).split(',')

        filters = {'template_name': scene_types}

        if values_to_check:
            for primary_filter in values_to_check:
                filters[kpi_template['param']] = primary_filter
                new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                result = self.calculate_category_space_length(**filters)
                score = result
                category_fk = self.scif[self.scif['category'] == primary_filter]['category_fk'].iloc[0]
                self.common.write_to_db_result(fk=135, numerator_id=category_fk, numerator_result=0,
                                           denominator_result= 0,
                                               denominator_id=999, result=result, score=score)
        else:
            result = self.calculate_category_space_length(**filters)
            score = result
            template_fk = self.scif[self.scif['template_name'] == scene_types]['template_fk'].iloc[0]
            self.common.write_to_db_result(fk= 136, numerator_id= template_fk,
                                           numerator_result=0,
                                           denominator_result= 0,
                                           denominator_id= 999, result=result, score=score)


    def calculate_category_space_length(self,  threshold=0.5,  **filters):
        """
        :param threshold: The ratio for a bay to be counted as part of a category.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """

        try:
            filtered_scif = self.scif[
                self.get_filter_condition(self.scif, **filters)]
            if self.EXCLUDE_EMPTY == True:
                    filtered_scif = filtered_scif[filtered_scif['product_type'] != 'Empty']

            space_length = 0
            bay_values = []
            max_linear_of_bays = 0
            product_fk_list = filtered_scif['product_fk'].unique().tolist()
            # space_length_DEBUG = 0
            for scene in filtered_scif['scene_fk'].unique().tolist():

                scene_matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
                scene_filters = filters
                scene_filters['scene_fk'] = scene
                scene_filters['product_fk'] = product_fk_list

                for bay in scene_matches['bay_number'].unique().tolist():
                    bay_total_linear = scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                         (scene_matches['stacking_layer'] == 1) &
                                                         (scene_matches['status'] == 1)]['width_mm_advance'].sum()
                    max_linear_of_bays += bay_total_linear
                    scene_filters['bay_number'] = bay
                    tested_group_linear = scene_matches[self.get_filter_condition(scene_matches, **scene_filters)]

                    tested_group_linear_value = tested_group_linear['width_mm_advance'].loc[tested_group_linear['stacking_layer'] == 1].sum()

                    if tested_group_linear_value:
                        bay_ratio = tested_group_linear_value / float(bay_total_linear)
                    else:
                        bay_ratio = 0

                    if bay_ratio >= threshold:
                        bay_values.append(4)
                    else:
                        bay_values.append(0)
                space_length = sum(bay_values)

        except Exception as e:
            Log.info('Linear Feet calculation failed due to {}'.format(e))
            space_length = 0

        return space_length


    def calculate_anchor(self,  kpi_name):
            template = self.Anchor_template.loc[self.Anchor_template['KPI'] == kpi_name]
            kpi_template = template.loc[template['KPI'] == kpi_name]
            if kpi_template.empty:
                return None
            kpi_template = kpi_template.iloc[0]

            values_to_check = []

            if kpi_template['param']:
                values_to_check = str(kpi_template['value']).split(',')

            filters = {kpi_template['param']: values_to_check}
            result = self.calculate_products_on_edge(**filters)
            score = 1 if result >= 1 else 0

            for value in values_to_check:
                sub_category_fk =  self.scif['sub_category_fk'].loc[self.scif['sub_category'] == value]
                sub_category_fk = sub_category_fk.iloc[0]


                self.common.write_to_db_result(fk=140, numerator_id=sub_category_fk,
                                           numerator_result=0,
                                           denominator_result=0,
                                           denominator_id=999, result=result, score=score)


    def calculate_products_on_edge(self, min_number_of_facings=1, min_number_of_shelves=1, **filters):
        """
        :param min_number_of_facings: Minimum number of edge facings for KPI to pass.
        :param min_number_of_shelves: Minimum number of different shelves with edge facings for KPI to pass.
        :param filters: This are the parameters which dictate the relevant SKUs for the edge calculation.
        :return: A tuple: (Number of scenes which pass, Total number of relevant scenes)
        """
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            return 0, 0
        number_of_edge_scenes = 0
        for scene in relevant_scenes:
            edge_facings = pd.DataFrame(columns=self.match_product_in_scene.columns)
            matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
            for shelf in matches['shelf_number'].unique():
                shelf_matches = matches[matches['shelf_number'] == shelf]
                if not shelf_matches.empty:
                    shelf_matches = shelf_matches.sort_values(by=['bay_number', 'facing_sequence_number'])
                    edge_facings = edge_facings.append(shelf_matches.iloc[0])
                    if len(edge_facings) > 1:
                        edge_facings = edge_facings.append(shelf_matches.iloc[-1])

            edge_facings = self.get_filter_condition(edge_facings, **filters)
            if edge_facings == None:
                edge_facings = 0
            elif edge_facings >= 1:
                return 1


        return edge_facings

    def separate_location_filters_from_product_filters(self, **filters):
        """
        This function gets scene-item-facts filters of all kinds, extracts the relevant scenes by the location filters,
        and returns them along with the product filters only.
        """
        location_filters = {}
        for field in filters.keys():
            if field not in self.all_products.columns and field in self.scif.columns:
                location_filters[field] = filters.pop(field)
        relevant_scenes = self.scif[self.get_filter_condition(self.scif, **location_filters)]['scene_id'].unique()
        return filters, relevant_scenes


    def calculate_eye_level(self, kpi_name):
        template = self.Eye_Level_template.loc[self.Eye_Level_template['KPI'] == kpi_name]
        kpi_template = template.loc[template['KPI'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]

        values_to_check = []

        if kpi_template['param']:
            values_to_check = str(kpi_template['value']).split(',')

        filters = {kpi_template['param']: values_to_check}
        result = self.calculate_eye_level_assortment(eye_level_configurations=self.eye_level_definition,
                                                                   min_number_of_products=1, percentage_result=True,
                                                                   requested_attribute='facings', **filters)
        score = 1 if result >= 1 else 0

        for value in values_to_check:
            sub_category_fk = self.scif['sub_category_fk'].loc[self.scif['sub_category'] == value]
            sub_category_fk = sub_category_fk.iloc[0]

            # self.common.write_to_db_result(fk=140, numerator_id=sub_category_fk,
            #                                numerator_result=0,
            #                                denominator_result=0,
            #                                denominator_id=999, result=result, score=score)

    def calculate_eye_level_assortment(self, eye_level_configurations=DEFAULT, min_number_of_products=ALL, **filters):
        """
        :param eye_level_configurations: A data frame containing information about shelves to ignore (==not eye level)
                                         for every number of shelves in each bay.
        :param min_number_of_products: Minimum number of eye level unique SKUs for KPI to pass.
        :param filters: This are the parameters which dictate the relevant SKUs for the eye-level calculation.
        :return: A tuple: (Number of scenes which pass, Total number of relevant scenes)
        """
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            return 0, 0
        if eye_level_configurations == self.DEFAULT:
            if hasattr(self, 'eye_level_configurations'):
                eye_level_configurations = self.eye_level_configurations
            else:
                Log.error('Eye-level configurations are not set up')
                return False
        number_of_products = len(self.all_products[self.get_filter_condition(self.all_products, **filters)]['product_ean_code'])
        min_shelf, max_shelf, min_ignore, max_ignore = eye_level_configurations.columns
        number_of_eye_level_scenes = 0
        for scene in relevant_scenes:
            eye_level_facings = pd.DataFrame(columns=self.match_product_in_scene.columns)
            matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
            for bay in matches['bay_number'].unique():
                bay_matches = matches[matches['bay_number'] == bay]
                number_of_shelves = bay_matches['shelf_number'].max()
                configuration = eye_level_configurations[(eye_level_configurations[min_shelf] <= number_of_shelves) &
                                                         (eye_level_configurations[max_shelf] >= number_of_shelves)]
                if not configuration.empty:
                    configuration = configuration.iloc[0]
                else:
                    configuration = {min_ignore: 0, max_ignore: 0}
                min_include = configuration[min_ignore] + 1
                max_include = number_of_shelves - configuration[max_ignore]
                eye_level_shelves = bay_matches[bay_matches['shelf_number'].between(min_include, max_include)]
                eye_level_facings = eye_level_facings.append(eye_level_shelves)
            eye_level_assortment = len(eye_level_facings[
                                           self.get_filter_condition(eye_level_facings, **filters)]['product_ean_code'])
            if min_number_of_products == self.ALL:
                min_number_of_products = number_of_products
            if eye_level_assortment >= min_number_of_products:
                number_of_eye_level_scenes += 1
        return number_of_eye_level_scenes, len(relevant_scenes)






    def kpi_name_builder(self, kpi_name, **filters):
        """
        This function builds kpi name according to naming convention
        """
        for filter in filters.keys():
            if filter == 'template_name':
                continue
            kpi_name = kpi_name.replace('{' + filter + '}', str(filters[filter]))
            kpi_name = kpi_name.replace("'", "\'")
        return kpi_name

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUPNGROGENERALToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
        :return: a filtered Scene Item Facts data frame.
        """
        if not filters:
            return df['pk'].apply(bool)
        if self.facings_field in df.keys():
            filter_condition = (df[self.facings_field] > 0)
        else:
            filter_condition = None
        for field in filters.keys():
            if field in df.keys():
                if isinstance(filters[field], tuple):
                    value, exclude_or_include = filters[field]
                else:
                    value, exclude_or_include = filters[field], self.INCLUDE_FILTER
                if not value:
                    continue
                if not isinstance(value, list):
                    value = [value]
                if exclude_or_include == self.INCLUDE_FILTER:
                    condition = (df[field].isin(value))
                elif exclude_or_include == self.EXCLUDE_FILTER:
                    condition = (~df[field].isin(value))
                elif exclude_or_include == self.CONTAIN_FILTER:
                    condition = (df[field].str.contains(value[0], regex=False))
                    for v in value[1:]:
                        condition |= df[field].str.contains(v, regex=False)
                else:
                    continue
                if filter_condition is None:
                    filter_condition = condition
                else:
                    filter_condition &= condition
            else:
                Log.warning('field {} is not in the Data Frame'.format(field))

        return filter_condition


