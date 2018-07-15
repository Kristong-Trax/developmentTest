import xlrd
import json
import pandas as pd
import numpy as np
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Trax.Utils.Logging.Logger import Log

from Projects.PNGAMERICA.Utils.PositionGraph import PNGAMERICAPositionGraphs

__author__ = 'Nimrod'


class PNGAMERICAGENERALToolBox:
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2
    EXCLUDE_EMPTY = False
    EXCLUDE_OTHER = False
    EXCLUDE_IRRELEVANT = False
    INCLUDE_OTHER = True
    INCLUDE_EMPTY = True

    STRICT_MODE = ALL = 1000

    EMPTY = 'Empty'
    DEFAULT = 'Default'
    TOP = 'Top'
    BOTTOM = 'Bottom'

    MM_TO_FEET_CONVERSION = 0.0032808399

    FABRICARE_CATEGORIES = ['TOTAL FABRIC CONDITIONERS', 'BLEACH AND LAUNDRY ADDITIVES', 'TOTAL LAUNDRY CARE']
    PG_CATEGORY = 'PG_CATEGORY'

    def __init__(self, data_provider, output, rds_conn=None, ignore_stacking=False, front_facing=False, **kwargs):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.rds_conn = rds_conn
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif['Sub Brand'] = self.scif['Sub Brand'].str.strip()
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.scenes_info = self.data_provider[Data.SCENES_INFO].merge(self.data_provider[Data.ALL_TEMPLATES],
                                                                      how='left', on='template_fk', suffixes=['', '_y'])
        self.ignore_stacking = ignore_stacking
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.front_facing = front_facing
        self.average_shelf_values = {}
        self.ps_dataprovider = PsDataProvider(data_provider, output)
        self.scif = self._filter_excluded_scene()
        for data in kwargs.keys():
            setattr(self, data, kwargs[data])
        if self.front_facing:
            self.scif = self.scif[self.scif['front_face_count'] == 1]

    @property
    def position_graphs(self):
        if not hasattr(self, '_position_graphs'):
            self._position_graphs = PNGAMERICAPositionGraphs(self.data_provider, rds_conn=self.rds_conn)
        return self._position_graphs

    @property
    def match_product_in_scene(self):
        if not hasattr(self, '_match_product_in_scene'):
            self._match_product_in_scene = self.position_graphs.match_product_in_scene
            if self.front_facing:
                self._match_product_in_scene = self._match_product_in_scene[
                    self._match_product_in_scene['front_facing'] == 'Y']
            if self.ignore_stacking:
                self._match_product_in_scene = self._match_product_in_scene[
                    self._match_product_in_scene['stacking_layer'] == 1]
        return self._match_product_in_scene

    def _filter_excluded_scene(self):
        excluded_scenes_df = self.ps_dataprovider.get_excluded_scenes()
        mask = self.scif['scene_id'].isin(excluded_scenes_df['pk'])
        return self.scif[~mask]

    def get_survey_answer(self, survey_data, answer_field=None):
        """
        :param survey_data:     1) str - The name of the survey in the DB.
                                2) tuple - (The field name, the field value). For example: ('question_fk', 13)
        :param answer_field: The DB field from which the answer is extracted. Default is the usual hierarchy.
        :return: The required survey response.
        """
        if not isinstance(survey_data, (list, tuple)):
            entity = 'question_text'
            value = survey_data
        else:
            entity, value = survey_data
        survey = self.survey_response[self.survey_response[entity] == value]
        if survey.empty:
            return None
        survey = survey.iloc[0]
        if answer_field is None or answer_field not in survey.keys():
            answer_field = 'selected_option_text' if survey['selected_option_text'] else 'number_value'
        survey_answer = survey[answer_field]
        return survey_answer

    def check_survey_answer(self, survey_text, target_answer):
        """
        :param survey_text:     1) str - The name of the survey in the DB.
                                2) tuple - (The field name, the field value). For example: ('question_fk', 13)
        :param target_answer: The required answer/s for the KPI to pass.
        :return: True if the answer matches the target; otherwise - False.
        """
        if not isinstance(survey_text, (list, tuple)):
            entity = 'question_text'
            value = survey_text
        else:
            entity, value = survey_text
        value = [value] if not isinstance(value, list) else value
        survey_data = self.survey_response[self.survey_response[entity].isin(value)]
        if survey_data.empty:
            Log.warning('Survey with {} = {} doesn\'t exist'.format(entity, value))
            return None
        answer_field = 'selected_option_text' if not survey_data['selected_option_text'].empty else 'number_value'
        target_answers = [target_answer] if not isinstance(target_answer, (list, tuple)) else target_answer
        survey_answers = survey_data[answer_field].values.tolist()
        for answer in target_answers:
            if answer in survey_answers:
                return True
        return False

    def calculate_number_of_scenes(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The number of scenes matching the filtered Scene Item Facts data frame.
        """
        if filters:
            if set(filters.keys()).difference(self.scenes_info.keys()):
                scene_data = self.scif[self.get_filter_condition(self.scif, **filters)]
            else:
                scene_data = self.scenes_info[self.get_filter_condition(self.scenes_info, **filters)]
        else:
            scene_data = self.scenes_info
        number_of_scenes = len(scene_data['scene_fk'].unique().tolist())
        return number_of_scenes

    def calculate_availability(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Total number of SKUs facings appeared in the filtered Scene Item Facts data frame.
        """
        if set(filters.keys()).difference(self.scif.keys()):
            filtered_df = self.match_product_in_scene[self.get_filter_condition(self.match_product_in_scene, **filters)]
        else:
            filtered_df = self.scif[self.get_filter_condition(self.scif, **filters)]
        if self.facings_field in filtered_df.columns:
            availability = filtered_df[self.facings_field].sum()
        else:
            availability = len(filtered_df)
        return availability

    def calculate_assortment(self, assortment_entity='product_ean_code', minimum_assortment_for_entity=1, **filters):
        """
        :param assortment_entity: This is the entity on which the assortment is calculated.
        :param minimum_assortment_for_entity: This is the number of assortment per each unique entity in order for it
                                              to be counted in the final assortment result (default is 1).
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Number of unique SKUs appeared in the filtered Scene Item Facts data frame.
        """
        if set(filters.keys()).difference(self.scif.keys()):
            filtered_df = self.match_product_in_scene[self.get_filter_condition(self.match_product_in_scene, **filters)]
        else:
            filtered_df = self.scif[self.get_filter_condition(self.scif, **filters)]
        if minimum_assortment_for_entity == 1:
            assortment = len(filtered_df[assortment_entity].unique())
        else:
            assortment = 0
            for entity_id in filtered_df[assortment_entity].unique():
                assortment_for_entity = filtered_df[filtered_df[assortment_entity] == entity_id]
                if self.facings_field in filtered_df.columns:
                    assortment_for_entity = assortment_for_entity[self.facings_field].sum()
                else:
                    assortment_for_entity = len(assortment_for_entity)
                if assortment_for_entity >= minimum_assortment_for_entity:
                    assortment += 1
        return assortment

    def calculate_share_of_shelf(self, sos_filters=None, include_empty=EXCLUDE_EMPTY, **general_filters):
        """
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The ratio of the Facings SOS.
        """
        if include_empty == self.EXCLUDE_EMPTY and 'product_type' not in sos_filters.keys() + general_filters.keys():
            general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)
        pop_filter = self.get_filter_condition(self.scif, **general_filters)
        subset_filter = self.get_filter_condition(self.scif, **sos_filters)

        try:
            ratio = self.k_engine.calculate_sos_by_facings(pop_filter=pop_filter, subset_filter=subset_filter)
        except:
            ratio = 0

        if not isinstance(ratio, (float, int)):
            ratio = 0
        return ratio

    def calculate_linear_share_of_shelf(self, sos_filters, include_empty=EXCLUDE_EMPTY, **general_filters):
        """
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The Linear SOS ratio.
        """
        if include_empty == self.EXCLUDE_EMPTY:
            general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)

        numerator_width = self.calculate_share_space_length(**dict(sos_filters, **general_filters))
        denominator_width = self.calculate_share_space_length(**general_filters)

        if denominator_width == 0:
            ratio = 0
        else:
            ratio = numerator_width / float(denominator_width)
        return ratio

    def calculate_share_space_length(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """
        try:
            if set(filters.keys()).difference(self.scif.keys()):
                filtered_df = self.match_product_in_scene[
                    self.get_filter_condition(self.match_product_in_scene, **filters)]
                space_length = filtered_df['width_mm_advance'].sum()
            else:
                filtered_df = self.scif[self.get_filter_condition(self.scif, **filters)]
                space_length = filtered_df['gross_len_ign_stack'].sum()
        except Exception as e:
            Log.info('Linear Feet calculation failed due to {}'.format(e))
            space_length = 0
        return space_length

    def calculate_category_space(self, kpi_name, threshold=0.5, retailer=None, exclude_pl=False,**filters):
        """
        :param threshold: The ratio for a bay to be counted as part of a category.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """
        try:
            filtered_scif = self.scif[
                self.get_filter_condition(self.scif, **filters)]
            space_length = 0
            bay_values = []
            for scene in filtered_scif['scene_fk'].unique().tolist():
                scene_matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
                scene_filters = filters
                scene_filters['scene_fk'] = scene
                for bay in scene_matches['bay_number'].unique().tolist():
                    bay_total_linear = scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                         (scene_matches['stacking_layer'] == 1) &
                                                         (scene_matches['status'] == 1)]['width_mm_advance'].sum()
                    scene_filters['bay_number'] = bay
                    tested_group_linear = scene_matches[self.get_filter_condition(scene_matches, **scene_filters)]
                    if exclude_pl:
                        tested_group_linear = tested_group_linear.loc[(tested_group_linear[self.PG_CATEGORY].isin(self.FABRICARE_CATEGORIES))]
                    tested_group_linear_value = tested_group_linear['width_mm_advance'].sum()
                    # tested_group_linear = self.calculate_share_space_length(**scene_filters)
                    if tested_group_linear_value:
                        bay_ratio = bay_total_linear / float(tested_group_linear_value)
                    else:
                        bay_ratio = 0
                    if bay_ratio >= threshold:
                        bay_num_of_shelves = len(scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                                   (scene_matches['stacking_layer'] == 1)][
                                                     'shelf_number'].unique().tolist())
                        if kpi_name not in self.average_shelf_values.keys():
                            self.average_shelf_values[kpi_name] = {'num_of_shelves': bay_num_of_shelves,
                                                                   'num_of_bays': 1}
                        else:
                            self.average_shelf_values[kpi_name]['num_of_shelves'] += bay_num_of_shelves
                            self.average_shelf_values[kpi_name]['num_of_bays'] += 1
                        if bay_num_of_shelves:
                            bay_final_linear_value = tested_group_linear_value / float(bay_num_of_shelves)
                        else:
                            bay_final_linear_value = 0
                        bay_values.append(bay_final_linear_value)
                        space_length += bay_final_linear_value
            if retailer in ['CVS', 'Walgreens']:
                if (sum([value*self.MM_TO_FEET_CONVERSION for value in bay_values if value > 0]) /
                        float(len([value*self.MM_TO_FEET_CONVERSION for value in bay_values if value > 0]))) < 3.2:
                    space_length = sum([3 for value in bay_values if value > 0])
            else:
                space_length = sum([4 for value in bay_values if value*self.MM_TO_FEET_CONVERSION > 1.5])
        except Exception as e:
            Log.info('Linear Feet calculation failed due to {}'.format(e))
            space_length = 0

        return space_length

    def calculate_share_space_length_new(self, threshold=0.5, retailer=None, exclude_pl=False,**filters):
        """
        :param threshold: The ratio for a bay to be counted as part of a category.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """
        try:
            filtered_scif = self.scif[
                self.get_filter_condition(self.scif, **filters)]
            space_length = 0
            for scene in filtered_scif['scene_fk'].unique().tolist():
                scene_matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
                scene_filters = filters
                scene_filters['scene_fk'] = scene
                for bay in scene_matches['bay_number'].unique().tolist():
                    bay_total_linear = scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                         (scene_matches['stacking_layer'] == 1) &
                                                         (scene_matches['status'] == 1)]['width_mm_advance'].sum()
                    scene_filters['bay_number'] = bay
                    tested_group_linear = scene_matches[self.get_filter_condition(scene_matches, **scene_filters)]
                    if exclude_pl:
                        tested_group_linear = tested_group_linear.loc[(tested_group_linear[self.PG_CATEGORY].isin(self.FABRICARE_CATEGORIES))]
                    tested_group_linear_value = tested_group_linear['width_mm_advance'].sum()
                    if tested_group_linear_value:
                        bay_ratio = bay_total_linear / float(tested_group_linear_value)
                    else:
                        bay_ratio = 0
                    if bay_ratio >= threshold:
                        space_length += tested_group_linear_value
        except Exception as e:
            Log.info('Linear Feet calculation failed due to {}'.format(e))
            space_length = 0

        return space_length

    def calculate_category_space_per_bay(self, bay_matches, testes_attributes, threshold=0.5, **filters):
        """
        :param threshold: The ratio for a bay to be counted as part of a category.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """

        bay_total_linear = bay_matches.loc[(bay_matches['stacking_layer'] == 1) &
                                           (bay_matches['status'] == 1)]['width_mm_advance'].sum()
        bay_matches['bay_number'] = bay_matches['bay_number'].values[0]
        for tested_attribute in testes_attributes:
            tested_group_linear = self.calculate_share_space_length(**filters)
            if tested_group_linear:
                bay_ratio = bay_total_linear / float(tested_group_linear)
            else:
                bay_ratio = 0
            if bay_ratio >= threshold:
                return tested_attribute
        return None

    def calculate_products_on_edge(self, min_number_of_facings=1, min_number_of_shelves=1, list_result=False,
                                   category=None,
                                   position=None, **filters):
        """
        :param min_number_of_facings: Minimum number of edge facings for KPI to pass.
        :param scene_filters: dict with params to filter the matches (ex-anchor of category...)
        :param min_number_of_shelves: Minimum number of different shelves with edge facings for KPI to pass.
        :param filters: This are the parameters which dictate the relevant SKUs for the edge calculation.
        :return: A tuple: (Number of scenes which pass, Total number of relevant scenes)
        """
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            return 0, 0
        number_of_edge_scenes = 0
        total_edge = pd.DataFrame(columns=self.match_product_in_scene.columns)
        for scene in relevant_scenes:
            edge_facings = pd.DataFrame(columns=self.match_product_in_scene.columns)
            matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
            if category:
                matches = self.match_product_in_scene[(self.match_product_in_scene['category'] == category) &
                                                      (self.match_product_in_scene['scene_fk'] == scene)]
            if not matches.empty:
                left_most_bay = min(matches['bay_number'].unique().tolist())
                right_most_bay = max(matches['bay_number'].unique().tolist())
                for shelf in matches['shelf_number'].unique():
                    shelf_matches = matches[matches['shelf_number'] == shelf]
                    if not shelf_matches.empty:
                        shelf_matches = shelf_matches.sort_values(by=['bay_number', 'facing_sequence_number'])
                        if position:
                            if position == 'left':
                                shelf_matches = shelf_matches.loc[shelf_matches['bay_number'] == left_most_bay]
                                if not shelf_matches.empty:
                                    edge_facings = edge_facings.append(shelf_matches.iloc[0])
                            if position == 'right':
                                shelf_matches = shelf_matches.loc[shelf_matches['bay_number'] == right_most_bay]
                                if not shelf_matches.empty:
                                    if len(edge_facings) - 1 <= len(shelf_matches):
                                        edge_facings = edge_facings.append(shelf_matches.iloc[-1])
                        else:
                            edge_facings = edge_facings.append(shelf_matches.iloc[0])
                            if len(edge_facings) > 1:
                                edge_facings = edge_facings.append(shelf_matches.iloc[-1])
                filters['product_type'] = 'SKU'
                edge_facings = edge_facings[self.get_filter_condition(edge_facings, **filters)]
                total_edge = total_edge.append(edge_facings)
                if len(edge_facings) >= min_number_of_facings \
                        and len(edge_facings['shelf_number'].unique()) >= min_number_of_shelves:
                    number_of_edge_scenes += 1
        if list_result:
            return total_edge
        else:
            return number_of_edge_scenes, len(relevant_scenes)

    def calculate_shelf_level_assortment(self, shelves, from_top_or_bottom=TOP, **filters):
        """
        :param shelves: A shelf number (of type int or string), or a list of shelves (of type int or string).
        :param from_top_or_bottom: TOP for default shelf number (counted from top)
                                    or BOTTOM for shelf number counted from bottom.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Number of unique SKUs appeared in the filtered condition.
        """
        shelves = shelves if isinstance(shelves, list) else [shelves]
        shelves = [int(shelf) for shelf in shelves]
        if from_top_or_bottom == self.TOP:
            assortment = self.calculate_assortment(shelf_number=shelves, **filters)
        else:
            assortment = self.calculate_assortment(shelf_number_from_bottom=shelves, **filters)
        return assortment

    def calculate_eye_level_assortment(self, eye_level_configurations=DEFAULT, min_number_of_products=ALL,
                                       percentage_result=False, requested_attribute=None, products_list=False,
                                       sub_category=False, **filters):
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
        # if eye_level_configurations == self.DEFAULT:
        #     if hasattr(self, 'eye_level_configurations'):
        #         eye_level_configurations = self.eye_level_configurations
        #     else:
        #         Log.error('Eye-level configurations are not set up')
        #         return False
        number_of_products = len(
            self.all_products[self.get_filter_condition(self.all_products, **filters)]['product_ean_code'])
        min_shelf, max_shelf, min_ignore, max_ignore = eye_level_configurations.columns
        number_of_eye_level_scenes = 0
        number_of_eye_level_entities = 0
        total_filtered_attributes = 0
        if percentage_result:
            filters['stacking_layer'] = 1
            total_filtered_attributes = self.calculate_share_space_length(**filters)
            del filters['stacking_layer']
        products_on_eye_level = []
        for scene in relevant_scenes:
            eye_level_facings = pd.DataFrame(columns=self.match_product_in_scene.columns)
            matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
            for bay in matches['bay_number'].unique():
                if bay >= 0:
                    try:
                        bay_matches = matches[matches['bay_number'] == bay]
                        if sub_category:
                            tested_attributes = eye_level_configurations['sub_category'].unique().tolist()
                            bay_sub_category = self.calculate_category_space_per_bay(bay_matches, tested_attributes,
                                                                                     **filters)
                            if bay_sub_category is not None:
                                eye_level_configurations = eye_level_configurations.loc[
                                    eye_level_configurations['sub_category'] == bay_sub_category]
                            else:
                                continue
                        number_of_shelves = bay_matches['shelf_number'].max()
                        configuration = eye_level_configurations[
                            (eye_level_configurations[min_shelf] <= number_of_shelves) &
                            (eye_level_configurations[max_shelf] >= number_of_shelves)]
                        if not configuration.empty:
                            configuration = configuration.iloc[0]
                        else:
                            configuration = {min_ignore: 0, max_ignore: 0}
                        min_include = configuration[min_ignore] + 1
                        max_include = number_of_shelves - configuration[max_ignore]
                        eye_level_shelves = bay_matches[bay_matches['shelf_number'].between(min_include, max_include)]
                        eye_level_facings = eye_level_facings.append(eye_level_shelves)
                        # if any(eye_level_shelves[eye_level_shelves['manufacturer_name'] == 'PROCTER & GAMBLE']['product_name'].unique()):
                        #     products_on_eye_level.append(eye_level_shelves[eye_level_shelves['manufacturer_name'] == 'PROCTER & GAMBLE']['product_name'].unique().tolist())
                        if any(eye_level_shelves[
                                   self.get_filter_condition(eye_level_shelves, **filters)]['product_ean_code']):
                            products_on_eye_level.append(eye_level_shelves[self.get_filter_condition(
                                eye_level_shelves, **filters)]['product_name'].unique().tolist())
                    except Exception as e:
                        Log.info('Adding Eye Level products failed for bay {} in scene {}'.format(bay, scene))
            # eye_level_assortment = len(eye_level_facings[
            #                                    self.get_filter_condition(eye_level_facings, **filters)]['product_ean_code'])
            eye_level_assortment = sum(eye_level_facings[
                                           self.get_filter_condition(eye_level_facings, **filters)]['width_mm_advance'])
            if percentage_result:
                number_of_eye_level_entities += eye_level_assortment
            if min_number_of_products == self.ALL:
                min_number_of_products = number_of_products
            if eye_level_assortment >= min_number_of_products:
                number_of_eye_level_scenes += 1
        if percentage_result:
            return number_of_eye_level_entities, total_filtered_attributes
        elif products_list:
            all_products_names = []
            for p_list in products_on_eye_level:
                for product_name in p_list:
                    all_products_names.append(product_name)
            return set(all_products_names)
        else:
            return number_of_eye_level_scenes, len(relevant_scenes)

    def shelf_level_assortment(self, shelf_target, min_number_of_products=1, strict=True, **filters):
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            relevant_scenes = self.scif['scene_fk'].unique().tolist()
        number_of_products = \
            self.all_products[self.get_filter_condition(self.all_products, **filters)][
                'product_ean_code'].unique().tolist()
        result = 0  # Default score is FALSE
        for scene in relevant_scenes:
            eye_level_facings = pd.DataFrame(columns=self.match_product_in_scene.columns)
            matches = pd.merge(self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene],
                               self.all_products, on=['product_fk'])
            for bay in matches['bay_number'].unique():
                bay_matches = matches[matches['bay_number'] == bay]
                products_in_target_shelf = bay_matches[(bay_matches['shelf_number'].isin(shelf_target)) & (
                    bay_matches['product_ean_code_x'].isin(number_of_products))]
                eye_level_facings = eye_level_facings.append(products_in_target_shelf)
            eye_level_assortment = len(eye_level_facings[
                                           self.get_filter_condition(eye_level_facings, **filters)][
                                           'product_ean_code_x'])
            if eye_level_assortment >= min_number_of_products:
                result = 1
        return result

    def get_scene_blocks(self, graph, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY, **filters):
        """
        This function is a sub-function for Block Together. It receives a graph and filters and returns a list of
        clusters.
        """
        relevant_vertices = set(self.filter_vertices_from_graph(graph, **filters))
        if allowed_products_filters:
            allowed_vertices = self.filter_vertices_from_graph(graph, **allowed_products_filters)
        else:
            allowed_vertices = set()

        if include_empty == self.EXCLUDE_EMPTY:
            empty_vertices = {v.index for v in graph.vs.select(product_type='Empty')}
            allowed_vertices = set(allowed_vertices).union(empty_vertices)

        all_vertices = {v.index for v in graph.vs}
        vertices_to_remove = all_vertices.difference(relevant_vertices.union(allowed_vertices))
        graph.delete_vertices(vertices_to_remove)
        # removing clusters including 'allowed' SKUs only
        blocks = [block for block in graph.clusters() if set(block).difference(allowed_vertices)]
        return blocks, graph

    def calculate_existence_of_blocks(self, conditions, include_empty=EXCLUDE_EMPTY, min_number_of_blocks=1, **filters):
        """
        :param conditions: A dictionary which contains assortment/availability conditions for filtering the blocks,
                           in the form of: {entity_type: (0 for assortment or 1 for availability,
                                                          a list of values =or None=,
                                                          minimum number of assortment/availability)}.
                           For example: {'product_ean_code': ('44545345434', 3)}
        :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
        :param min_number_of_blocks: The number of blocks needed in order for the KPI to pass.
                                     If all appearances are required: == self.ALL.
        :param filters: These are the parameters which the blocks are checked for.
        :return: The number of blocks (from all scenes) which match the filters and conditions.
        """

        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
            return 0

        number_of_blocks = 0
        for scene in relevant_scenes:
            scene_graph = self.position_graphs.get(scene).copy()
            blocks, scene_graph = self.get_scene_blocks(scene_graph, allowed_products_filters=None,
                                                        include_empty=include_empty, **filters)
            for block in blocks:
                entities_data = {entity: [] for entity in conditions.keys()}
                for vertex in block:
                    vertex_attributes = scene_graph.vs[vertex].attributes()
                    for entity in conditions.keys():
                        entities_data[entity].append(vertex_attributes[entity])

                block_successful = True
                for entity in conditions.keys():
                    assortment_or_availability, values, minimum_result = conditions[entity]
                    if assortment_or_availability == 0:
                        if values:
                            result = len(set(entities_data[entity]).intersection(values))
                        else:
                            result = len(set(entities_data[entity]))
                    elif assortment_or_availability == 1:
                        if values:
                            result = len([facing for facing in entities_data if facing in values])
                        else:
                            result = len(entities_data[entity])
                    else:
                        continue
                    if result < minimum_result:
                        block_successful = False
                        break
                if block_successful:
                    number_of_blocks += 1
                    if number_of_blocks >= min_number_of_blocks:
                        return number_of_blocks
                else:
                    if min_number_of_blocks == self.ALL:
                        return 0

        if number_of_blocks >= min_number_of_blocks or min_number_of_blocks == self.ALL:
            return number_of_blocks
        return 0

    def calculate_product_sequence(self, sequence_filters, direction, empties_allowed=True, irrelevant_allowed=False,
                                   min_required_to_pass=STRICT_MODE, custom_graph=None, **general_filters):
        """
        :param sequence_filters: One of the following:
                        1- a list of dictionaries, each containing the filters values of an organ in the sequence.
                        2- a tuple of (entity_type, [value1, value2, value3...]) in case every organ in the sequence
                           is defined by only one filter (and of the same entity, such as brand_name, etc).
        :param direction: left/right/top/bottom - the direction of the sequence.
        :param empties_allowed: This dictates whether or not the sequence can be interrupted by Empty facings.
        :param irrelevant_allowed: This dictates whether or not the sequence can be interrupted by facings which are
                                   not in the sequence.
        :param min_required_to_pass: The number of sequences needed to exist in order for KPI to pass.
                                     If STRICT_MODE is activated, the KPI passes only if it has NO rejects.
        :param custom_graph: A filtered Positions graph - given in case only certain vertices need to be checked.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: True if the KPI passes; otherwise False.
        """
        if isinstance(sequence_filters, (list, tuple)) and isinstance(sequence_filters[0], (str, unicode)):
            sequence_filters = [{sequence_filters[0]: values} for values in sequence_filters[1]]
        new_sequence_filters = []
        for filters in sequence_filters:
            for value in filters.values():
                try:
                    value = int(value)
                except ValueError as e:
                    value = value
            filters[filters.keys()[0]] = value
            new_sequence_filters.append(filters)

        pass_counter = 0
        reject_counter = 0

        if not custom_graph:

            filtered_scif = self.scif[self.get_filter_condition(self.scif, **general_filters)]
            scenes = set(filtered_scif['scene_id'].unique())
            scenes_to_add = []
            filters_presence_dict = {}
            for filters in new_sequence_filters:
                scene_for_filters = filtered_scif[self.get_filter_condition(filtered_scif, **filters)][
                    'scene_id'].unique().tolist()
                # scenes = scenes.intersection(scene_for_filters)
                scenes_to_add.extend(scene_for_filters)
                # if not scenes:
                #     Log.debug('None of the scenes include products from all types relevant for sequence')
                #     return False
                if not scene_for_filters:
                    filters_presence_dict[filters.values()[0]] = 0
                    Log.info('Filter {} does not exist in the visit'.format(filters))
                    # return False
                else:
                    filters_presence_dict[filters.values()[0]] = 1
            # if not (sum(filters_presence_dict.values()) / float(len(sequence_filters)) >= 0.8):
            #     Log.debug('None of the scenes include products from all types relevant for sequence')
            #     return False
            scenes_to_check = set(scenes) & set(scenes_to_add)
            for scene in scenes_to_check:
                scene_graph = self.position_graphs.get(scene)
                scene_matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
                bays = scene_matches['bay_number'].unique().tolist()
                for bay in bays:
                    bay_shelves = scene_matches.loc[scene_matches['bay_number'] == bay]
                    for shelf in bay_shelves:
                        sequence_filters.append({'bay_number': bay})
                        sequence_filters.append({'shelf_number': shelf})
                        scene_passes, scene_rejects = self.calculate_sequence_for_graph(scene_graph, sequence_filters,
                                                                                        direction,
                                                                                        empties_allowed,
                                                                                        irrelevant_allowed)
                        pass_counter += scene_passes
                        reject_counter += scene_rejects

                if reject_counter > 0 or pass_counter == 0:
                    return False
                elif pass_counter > 0:
                    return True
                    # if pass_counter >= min_required_to_pass:
                    #     return True
                    # elif min_required_to_pass == self.STRICT_MODE and reject_counter > 0:
                    #     return False

        else:
            scene_passes, scene_rejects = self.calculate_sequence_for_graph(custom_graph, sequence_filters, direction,
                                                                            empties_allowed, irrelevant_allowed)
            pass_counter += scene_passes
            reject_counter += scene_rejects

        if pass_counter >= min_required_to_pass or reject_counter == 0:
            return True
        else:
            return False

    def calculate_sequence_for_graph(self, graph, sequence_filters, direction, empties_allowed, irrelevant_allowed):
        """
        This function checks for a sequence given a position graph (either a full scene graph or a customized one).
        """
        pass_counter = 0
        reject_counter = 0

        # removing unnecessary edges
        filtered_scene_graph = graph.copy()
        if len(filtered_scene_graph.es) == 0:
            return pass_counter, reject_counter
        edges_to_remove = filtered_scene_graph.es.select(direction_ne=direction)
        filtered_scene_graph.delete_edges([edge.index for edge in edges_to_remove])

        reversed_scene_graph = graph.copy()
        edges_to_remove = reversed_scene_graph.es.select(direction_ne=self._reverse_direction(direction))
        reversed_scene_graph.delete_edges([edge.index for edge in edges_to_remove])

        vertices_list = []
        for filters in sequence_filters:
            vertices_list.append(self.filter_vertices_from_graph(graph, **filters))
        tested_vertices, sequence_vertices = vertices_list[0], vertices_list[1:]
        vertices_list = reduce(lambda x, y: x + y, sequence_vertices)

        sequences = []
        for vertex in tested_vertices:
            previous_sequences = self.get_positions_by_direction(reversed_scene_graph, vertex)
            if previous_sequences and set(vertices_list).intersection(reduce(lambda x, y: x + y, previous_sequences)):
                reject_counter += 1
                continue

            next_sequences = self.get_positions_by_direction(filtered_scene_graph, vertex)
            sequences.extend(next_sequences)

        sequences = self._filter_sequences(sequences)
        for sequence in sequences:
            all_products_appeared = True
            empties_found = False
            irrelevant_found = False
            full_sequence = False
            broken_sequence = False
            current_index = 0
            previous_vertices = list(tested_vertices)

            for vertices in sequence_vertices:
                if not set(sequence).intersection(vertices):
                    all_products_appeared = False
                    break

            for vindex in sequence:
                vertex = graph.vs[vindex]
                if vindex not in vertices_list and vindex not in tested_vertices:
                    if current_index < len(sequence_vertices):
                        if vertex['product_type'] == self.EMPTY:
                            empties_found = True
                        else:
                            irrelevant_found = True
                elif vindex in previous_vertices:
                    pass
                elif vindex in sequence_vertices[current_index]:
                    previous_vertices = list(sequence_vertices[current_index])
                    current_index += 1
                else:
                    broken_sequence = True

            if current_index == len(sequence_vertices):
                full_sequence = True

            if broken_sequence:
                reject_counter += 1
            elif full_sequence:
                if not empties_allowed and empties_found:
                    reject_counter += 1
                elif not irrelevant_allowed and irrelevant_found:
                    reject_counter += 1
                elif all_products_appeared:
                    pass_counter += 1
        return pass_counter, reject_counter

    def calculate_product_sequence_per_shelf(self, sequence_filters, direction, filter_attributes_index_dict,
                                             empties_allowed=True, irrelevant_allowed=False,
                                             min_required_to_pass=STRICT_MODE, custom_graph=None,
                                             reject_threshold=0.05, **general_filters):
        """
        :param sequence_filters: One of the following:
                        1- a list of dictionaries, each containing the filters values of an organ in the sequence.
                        2- a tuple of (entity_type, [value1, value2, value3...]) in case every organ in the sequence
                           is defined by only one filter (and of the same entity, such as brand_name, etc).
        :param direction: left/right/top/bottom - the direction of the sequence.
        :param empties_allowed: This dictates whether or not the sequence can be interrupted by Empty facings.
        :param irrelevant_allowed: This dictates whether or not the sequence can be interrupted by facings which are
                                   not in the sequence.
        :param min_required_to_pass: The number of sequences needed to exist in order for KPI to pass.
                                     If STRICT_MODE is activated, the KPI passes only if it has NO rejects.
        :param custom_graph: A filtered Positions graph - given in case only certain vertices need to be checked.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: True if the KPI passes; otherwise False.
        """
        if isinstance(sequence_filters, (list, tuple)) and isinstance(sequence_filters[0], (str, unicode)):
            sequence_filters = [{sequence_filters[0]: values} for values in sequence_filters[1]]
        new_sequence_filters = []
        total_values = []

        for filters in sequence_filters:
            filter_key = filters.keys()[0]
            for value in filters.values():
                # try:
                #     value = int(value)
                # except ValueError as e:
                #     value = value
                total_values.append(value)
            filters[filters.keys()[0]] = value
            new_sequence_filters.append(filters)
        final_filter_key = filter_key

        pass_counter = 0
        reject_counter = 0

        filtered_scif = self.scif[self.get_filter_condition(self.scif, **general_filters)]
        scenes = set(filtered_scif['scene_id'].unique())
        scenes_to_add = []
        filters_presence_dict = {}
        for filters in new_sequence_filters:
            scene_for_filters = filtered_scif[self.get_filter_condition(filtered_scif, **filters)][
                'scene_id'].unique().tolist()
            # scenes = scenes.intersection(scene_for_filters)
            scenes_to_add.extend(scene_for_filters)
            # if not scenes:
            #     Log.debug('None of the scenes include products from all types relevant for sequence')
            #     return False
            if not scene_for_filters:
                filters_presence_dict[filters.values()[0]] = 0
                Log.info('Filter {} does not exist in the visit'.format(filters))
                # return False
            else:
                filters_presence_dict[filters.values()[0]] = 1
        # if not (sum(filters_presence_dict.values()) / float(len(sequence_filters)) >= 0.8):
        #     Log.debug('None of the scenes include products from all types relevant for sequence')
        #     return False
        if scenes_to_add:
            scenes_to_check = set(scenes) & set(scenes_to_add)
        else:
            scenes_to_check = scenes
        total_shelves = 0
        filters_for_availability = general_filters
        filters_for_availability[final_filter_key] = [str(value) for value in total_values]
        number_of_facings = self.calculate_availability(**filters_for_availability)
        for scene in scenes_to_check:

            # scene_graph = self.position_graphs.get(scene)
            scene_matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
            bays = scene_matches['bay_number'].unique().tolist()
            for bay in bays:
                bay_shelves = scene_matches.loc[scene_matches['bay_number'] == bay]
                if bay_shelves.loc[bay_shelves[final_filter_key].isin(total_values)].empty or \
                        bay_shelves[self.get_filter_condition(bay_shelves, **general_filters)].empty:
                    continue
                for shelf in bay_shelves['shelf_number'].unique().tolist():
                    shelf_rejected = False
                    last_sequence = 0
                    shelf_matches = scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                      (scene_matches['shelf_number'] == shelf)]
                    if shelf_matches.loc[shelf_matches[final_filter_key].isin(total_values)].empty or \
                            shelf_matches[self.get_filter_condition(shelf_matches, **general_filters)].empty:
                        continue
                    total_shelves += 1
                    for sequence in shelf_matches['facing_sequence_number'].unique().tolist():
                        sequence_matches = shelf_matches.loc[shelf_matches['facing_sequence_number'] == sequence]
                        if sequence_matches[self.get_filter_condition(sequence_matches, **general_filters)].empty:
                            continue
                        sequence_filter_value = shelf_matches.loc[
                            shelf_matches['facing_sequence_number'] == sequence][filter_key].values[0]
                        if sequence_filter_value in total_values:
                            if not type(sequence_filter_value) == str:
                                sequence_filter_value = str(sequence_filter_value)
                            product_index = filter_attributes_index_dict[sequence_filter_value]
                            if not product_index >= last_sequence:
                                shelf_rejected = True
                                continue
                            last_sequence = product_index
                    if shelf_rejected:
                        reject_counter += 1
                    else:
                        pass_counter += 1
        if not number_of_facings:
            return False
        reject_ratio = reject_counter / float(number_of_facings)
        if reject_ratio > reject_threshold or pass_counter == 0:
            return False
        else:
            return True
            # if pass_counter >= min_required_to_pass:
            #     return True
            # elif min_required_to_pass == self.STRICT_MODE and reject_counter > 0:
            #     return False

    def calculate_vertical_product_sequence_per_bay(self, sequence_filters, direction, filter_attributes_index_dict,
                                                    empties_allowed=True, irrelevant_allowed=False,
                                                    min_required_to_pass=STRICT_MODE, custom_graph=None,
                                                    allowed_sequence_fouls=1,
                                                    reject_threshold=0.05, **general_filters):
        """
        :param sequence_filters: One of the following:
                        1- a list of dictionaries, each containing the filters values of an organ in the sequence.
                        2- a tuple of (entity_type, [value1, value2, value3...]) in case every organ in the sequence
                           is defined by only one filter (and of the same entity, such as brand_name, etc).
        :param direction: left/right/top/bottom - the direction of the sequence.
        :param empties_allowed: This dictates whether or not the sequence can be interrupted by Empty facings.
        :param irrelevant_allowed: This dictates whether or not the sequence can be interrupted by facings which are
                                   not in the sequence.
        :param min_required_to_pass: The number of sequences needed to exist in order for KPI to pass.
                                     If STRICT_MODE is activated, the KPI passes only if it has NO rejects.
        :param custom_graph: A filtered Positions graph - given in case only certain vertices need to be checked.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: True if the KPI passes; otherwise False.
        """
        if isinstance(sequence_filters, (list, tuple)) and isinstance(sequence_filters[0], (str, unicode)):
            sequence_filters = [{sequence_filters[0]: values} for values in sequence_filters[1]]
        new_sequence_filters = []
        total_values = []

        for filters in sequence_filters:
            filter_key = filters.keys()[0]
            for value in filters.values():
                # try:
                #     value = int(value)
                # except ValueError as e:
                #     value = value
                total_values.append(value)
            filters[filters.keys()[0]] = value
            new_sequence_filters.append(filters)
        final_filter_key = filter_key

        pass_counter = 0
        reject_counter = 0

        filtered_scif = self.scif[self.get_filter_condition(self.scif, **general_filters)]
        scenes = set(filtered_scif['scene_id'].unique())
        scenes_to_add = []
        filters_presence_dict = {}
        for filters in new_sequence_filters:
            scene_for_filters = filtered_scif[self.get_filter_condition(filtered_scif, **filters)][
                'scene_id'].unique().tolist()
            # scenes = scenes.intersection(scene_for_filters)
            scenes_to_add.extend(scene_for_filters)
            # if not scenes:
            #     Log.debug('None of the scenes include products from all types relevant for sequence')
            #     return False
            if not scene_for_filters:
                filters_presence_dict[filters.values()[0]] = 0
                Log.info('Filter {} does not exist in the visit'.format(filters))
                # return False
            else:
                filters_presence_dict[filters.values()[0]] = 1

        # if not (sum(filters_presence_dict.values()) / float(len(sequence_filters)) >= 0.8):
        #     Log.debug('None of the scenes include products from all types relevant for sequence')
        #     return False
        filters_for_availability = general_filters
        filters_for_availability[final_filter_key] = [str(value) for value in total_values]
        number_of_facings = self.calculate_availability(**filters_for_availability)
        if scenes_to_add:
            scenes_to_check = set(scenes) & set(scenes_to_add)
        else:
            scenes_to_check = scenes
        total_shelves = 0
        for scene in scenes_to_check:

            # scene_graph = self.position_graphs.get(scene)
            scene_matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
            bays = scene_matches['bay_number'].unique().tolist()
            for bay in bays:
                bay_shelves = scene_matches.loc[scene_matches['bay_number'] == bay]
                if bay_shelves.loc[bay_shelves[final_filter_key].isin(total_values)].empty or \
                        bay_shelves[self.get_filter_condition(bay_shelves, **general_filters)].empty:
                    continue
                bay_attributes = []
                for shelf in bay_shelves['shelf_number'].unique().tolist():
                    shelf_rejected = False
                    shelf_matches = scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                      (scene_matches['shelf_number'] == shelf)]
                    if shelf_matches.loc[shelf_matches[final_filter_key].isin(total_values)].empty or \
                            shelf_matches[self.get_filter_condition(shelf_matches, **general_filters)].empty:
                        continue
                    shelf_attributes = shelf_matches[final_filter_key].unique().tolist()
                    bay_attributes.append(shelf_attributes)
                if direction == 'bottom':
                    iteration_list = reversed(bay_attributes)
                else:
                    iteration_list = bay_attributes
                min_value = 0
                max_value = 0
                last_max_value = 0
                sequence_indicator = False
                fouls_allowed_counter = 0
                for attributes in iteration_list:
                    if attributes:
                        min_value = min(attributes)
                        max_value = max(attributes)
                        attributes.remove(max_value)
                        second_max_value = max(attributes)
                        if min_value > last_max_value:
                            sequence_indicator = True
                            last_max_value = max_value
                        elif min_value > second_max_value:
                            sequence_indicator = True
                            last_max_value = max_value
                            fouls_allowed_counter += 1
                        else:
                            sequence_indicator = False

                if sequence_indicator:
                    pass_counter += 1
                else:
                    reject_counter += 1
        if not number_of_facings:
            return False
        reject_ratio = reject_counter / float(number_of_facings)
        if reject_ratio > reject_threshold or pass_counter == 0:
            return False
        else:
            return True

    @staticmethod
    def _reverse_direction(direction):
        """
        This function returns the opposite of a given direction.
        """
        if direction == 'top':
            new_direction = 'bottom'
        elif direction == 'bottom':
            new_direction = 'top'
        elif direction == 'left':
            new_direction = 'right'
        elif direction == 'right':
            new_direction = 'left'
        else:
            new_direction = direction
        return new_direction

    def get_positions_by_direction(self, graph, vertex_index):
        """
        This function gets a filtered graph (contains only edges of a relevant direction) and a Vertex index,
        and returns all sequences starting in it (until it gets to a dead end).
        """
        sequences = []
        edges = [graph.es[e] for e in graph.incident(vertex_index)]
        next_vertices = [edge.target for edge in edges]
        for vertex in next_vertices:
            next_sequences = self.get_positions_by_direction(graph, vertex)
            if not next_sequences:
                sequences.append([vertex])
            else:
                for sequence in next_sequences:
                    sequences.append([vertex] + sequence)
        return sequences

    @staticmethod
    def _filter_sequences(sequences):
        """
        This function receives a list of sequences (lists of indexes), and removes sequences which can be represented
        by a shorter sequence (which is also in the list).
        """
        if not sequences:
            return sequences
        sequences = sorted(sequences, key=lambda x: (x[-1], len(x)))
        filtered_sequences = [sequences[0]]
        for sequence in sequences[1:]:
            if sequence[-1] != filtered_sequences[-1][-1]:
                filtered_sequences.append(sequence)
        return filtered_sequences

    def calculate_non_proximity(self, tested_filters, anchor_filters, allowed_diagonal=False, **general_filters):
        """
        :param tested_filters: The tested SKUs' filters.
        :param anchor_filters: The anchor SKUs' filters.
        :param allowed_diagonal: True - a tested SKU can be in a direct diagonal from an anchor SKU in order
                                        for the KPI to pass;
                                 False - a diagonal proximity is NOT allowed.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return:
        """
        direction_data = []
        if allowed_diagonal:
            direction_data.append({'top': (0, 1), 'bottom': (0, 1)})
            direction_data.append({'right': (0, 1), 'left': (0, 1)})
        else:
            direction_data.append({'top': (0, 1), 'bottom': (0, 1), 'right': (0, 1), 'left': (0, 1)})
        is_proximity = self.calculate_relative_position(tested_filters, anchor_filters, direction_data,
                                                        min_required_to_pass=1, **general_filters)
        return not is_proximity

    def calculate_relative_in_block_per_graph(self, scene_graph, direction_data, min_required_to_pass=1,
                                              sent_tested_vertices=None, sent_anchor_vertices=None):

        """
        :param direction_data: The allowed distance between the tested and anchor SKUs.
                               In form: {'top': 4, 'bottom: 0, 'left': 100, 'right': 0}
                               Alternative form: {'top': (0, 1), 'bottom': (1, 1000), ...} - As range.
        :param min_required_to_pass: The number of appearances needed to be True for relative position in order for KPI
                                     to pass. If all appearances are required: ==a string or a big number.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: True if (at least) one pair of relevant SKUs fits the distance requirements; otherwise - returns False.
        """
        pass_counter = 0
        reject_counter = 0
        if sent_anchor_vertices and sent_tested_vertices:
            tested_vertices = sent_tested_vertices
            anchor_vertices = sent_anchor_vertices
        else:
            tested_vertices = self.filter_vertices_from_graph(scene_graph)
            anchor_vertices = self.filter_vertices_from_graph(scene_graph)
        for tested_vertex in tested_vertices:
            for anchor_vertex in anchor_vertices:
                moves = {'top': 0, 'bottom': 0, 'left': 0, 'right': 0}
                path = scene_graph.get_shortest_paths(anchor_vertex, tested_vertex, output='epath')
                if path:
                    path = path[0]
                    for edge in path:
                        moves[scene_graph.es[edge]['direction']] += 1
                    else:
                        result = self.validate_block_moves(moves=moves, direction_data=direction_data)
                    if result:
                        pass_counter += 1
                        if isinstance(min_required_to_pass, int) and pass_counter >= min_required_to_pass:
                            return True
                    else:
                        reject_counter += 1
        if pass_counter > 0 and reject_counter == 0:
            return True
        else:
            return False

    def calculate_adjacency_relativeness(self, tested_filters, direction_data,
                                         **general_filters):
        """
        :param tested_filters: The tested SKUs' filters.
        :param anchor_filters: The anchor SKUs' filters.
        :param direction_data: The allowed distance between the tested and anchor SKUs.
                               In form: {'top': 4, 'bottom: 0, 'left': 100, 'right': 0}
                               Alternative form: {'top': (0, 1), 'bottom': (1, 1000), ...} - As range.
        :param min_required_to_pass: The number of appearances needed to be True for relative position in order for KPI
                                     to pass. If all appearances are required: ==a string or a big number.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: True if (at least) one pair of relevant SKUs fits the distance requirements; otherwise - returns False.
        """
        filtered_scif = self.scif[self.get_filter_condition(self.scif, **general_filters)]
        tested_scenes = filtered_scif[self.get_filter_condition(filtered_scif, **tested_filters)]['scene_id'].unique().tolist()
        if tested_scenes:
            pass_counter = []
            for scene in tested_scenes:
                scene_graph = self.position_graphs.get(scene)
                all_vertices = {v.index for v in scene_graph.vs}
                tested_vertices = self.filter_vertices_from_graph(scene_graph, **tested_filters)
                anchor_vertices = all_vertices.difference(tested_vertices)
                for tested_vertex in tested_vertices:
                    for anchor_vertex in anchor_vertices:
                        moves = {'top': 0, 'bottom': 0, 'left': 0, 'right': 0}
                        path = scene_graph.get_shortest_paths(anchor_vertex, tested_vertex, output='epath')
                        if path:
                            path = path[0]
                            for edge in path:
                                moves[scene_graph.es[edge]['direction']] += 1
                            if self.validate_block_moves(moves, direction_data):
                                pass_counter.append(scene_graph.vs[anchor_vertex]['brand_name'])
                        else:
                            Log.debug('Tested and Anchor have no direct path')
            return pass_counter

    def calculate_relative_position(self, tested_filters, anchor_filters, direction_data, min_required_to_pass=1,
                                    **general_filters):
        """
        :param tested_filters: The tested SKUs' filters.
        :param anchor_filters: The anchor SKUs' filters.
        :param direction_data: The allowed distance between the tested and anchor SKUs.
                               In form: {'top': 4, 'bottom: 0, 'left': 100, 'right': 0}
                               Alternative form: {'top': (0, 1), 'bottom': (1, 1000), ...} - As range.
        :param min_required_to_pass: The number of appearances needed to be True for relative position in order for KPI
                                     to pass. If all appearances are required: ==a string or a big number.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: True if (at least) one pair of relevant SKUs fits the distance requirements; otherwise - returns False.
        """
        filtered_scif = self.scif[self.get_filter_condition(self.scif, **general_filters)]
        tested_scenes = filtered_scif[self.get_filter_condition(filtered_scif, **tested_filters)]['scene_id'].unique()
        anchor_scenes = filtered_scif[self.get_filter_condition(filtered_scif, **anchor_filters)]['scene_id'].unique()
        relevant_scenes = set(tested_scenes).intersection(anchor_scenes)

        if relevant_scenes:
            pass_counter = 0
            reject_counter = 0
            for scene in relevant_scenes:
                scene_graph = self.position_graphs.get(scene)
                tested_vertices = self.filter_vertices_from_graph(scene_graph, **tested_filters)
                anchor_vertices = self.filter_vertices_from_graph(scene_graph, **anchor_filters)
                for tested_vertex in tested_vertices:
                    for anchor_vertex in anchor_vertices:
                        moves = {'top': 0, 'bottom': 0, 'left': 0, 'right': 0}
                        path = scene_graph.get_shortest_paths(anchor_vertex, tested_vertex, output='epath')
                        if path:
                            path = path[0]
                            for edge in path:
                                moves[scene_graph.es[edge]['direction']] += 1
                            if self.validate_moves(moves, direction_data):
                                pass_counter += 1
                                if isinstance(min_required_to_pass, int) and pass_counter >= min_required_to_pass:
                                    return True
                            else:
                                reject_counter += 1
                        else:
                            Log.debug('Tested and Anchor have no direct path')
            if pass_counter > 0 and reject_counter == 0:
                return True
            else:
                return False
        else:
            Log.debug('None of the scenes contain both anchor and tested SKUs')
            return False

    def filter_vertices_from_graph(self, graph, **filters):
        """
        This function is given a graph and returns a set of vertices calculated by a given set of filters.
        """
        vertices_indexes = None
        for field in filters.keys():
            field_vertices = set()
            values = filters[field] if isinstance(filters[field], (list, tuple)) else [filters[field]]
            for value in values:
                vertices = [v.index for v in graph.vs.select(**{field: value})]
                field_vertices = field_vertices.union(vertices)
            if vertices_indexes is None:
                vertices_indexes = field_vertices
            else:
                vertices_indexes = vertices_indexes.intersection(field_vertices)
        vertices_indexes = vertices_indexes if vertices_indexes is not None else [v.index for v in graph.vs]
        if self.front_facing:
            front_facing_vertices = [v.index for v in graph.vs.select(front_facing='Y')]
            vertices_indexes = set(vertices_indexes).intersection(front_facing_vertices)
        return list(vertices_indexes)

    @staticmethod
    def validate_moves(moves, direction_data):
        """
        This function checks whether the distance between the anchor and the tested SKUs fits the requirements.
        """
        direction_data = direction_data if isinstance(direction_data, (list, tuple)) else [direction_data]
        validated = False
        for data in direction_data:
            data_validated = True
            for direction in moves.keys():
                allowed_moves = data.get(direction, (0, 0))
                min_move, max_move = allowed_moves if isinstance(allowed_moves, tuple) else (0, allowed_moves)
                if not min_move <= moves[direction] <= max_move:
                    data_validated = False
                    break
            if data_validated:
                validated = True
                break
        return validated

    @staticmethod
    def validate_block_moves(moves, direction_data):
        """
        This function checks whether the distance between the anchor and the tested SKUs fits the requirements.
        """
        key = direction_data.keys()
        direction_data = direction_data if isinstance(direction_data, (list, tuple)) else [direction_data]
        one_to_pass = {}
        for data in direction_data:
            for direction in set(moves.keys()).intersection(key):
                allowed_moves = data.get(direction, (0, 0))
                min_move, max_move = allowed_moves if isinstance(allowed_moves, tuple) else (0, allowed_moves)
                if min_move <= moves[direction] <= max_move:
                    one_to_pass[direction] = 'T'
        if one_to_pass:
            if len(one_to_pass.values()) == 1:
                moves.pop(one_to_pass.keys()[0])
                for direction in moves.keys():
                    min_move, max_move = (0, 0)
                    if not min_move <= moves[direction] <= max_move:
                        return False
                return True
        return False

    def calculate_average_shelf(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """
        filtered_matches = self.match_product_in_scene[
            self.get_filter_condition(self.match_product_in_scene, **filters)]
        if not filtered_matches.empty:
            average_shelves = sum(filtered_matches['shelf_number'].values.tolist()) / \
                              float(len(filtered_matches['shelf_number'].values.tolist()))
        else:
            average_shelves = None
        return average_shelves

    def calculate_block_together(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY,
                                 include_other=EXCLUDE_OTHER, include_irrelevant=EXCLUDE_IRRELEVANT,
                                 minimum_block_ratio=1, result_by_scene=False, vertical=False, horizontal=False,
                                 orphan=False, group=False, block_of_blocks=False, block_products1=None,
                                 block_products2=None,
                                 block_products=None, group_products=None, include_private_label=False,
                                 availability_param=None, availability_value=None, color_wheel=False,
                                 checkerboard=False, **filters):
        """
        :param group_products: if we searching for group in block - this is the filter of the group inside the big block
        :param block_products: if we searching for group in block - this is the filter of the big block
        :param group: True if the kpi is for block of product inside a bigger block
        :param orphan: True if searching for orphan products 3 products away from the biggest block
        :param horizontal: True if the biggest block have to be at least 2X2
        :param vertical: True
        :param allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
        :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
        :param minimum_block_ratio: The minimum (block number of facings / total number of relevant facings) ratio
                                    in order for KPI to pass (if ratio=1, then only one block is allowed).
        :param result_by_scene: True - The result is a tuple of (number of passed scenes, total relevant scenes);
                                False - The result is True if at least one scene has a block, False - otherwise.
        :param filters: These are the parameters which the blocks are checked for.
        :return: see 'result_by_scene' above.
        """
        if block_products is None:
            block_products = {}
        if block_products1 is None:
            block_products1 = {}
        if block_products2 is None:
            block_products2 = {}
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            if result_by_scene:
                return 0, 0
            else:
                Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
                result = 'no_products'
                return result
        number_of_blocked_scenes = 0
        cluster_ratios = []
        for scene in relevant_scenes:
            scene_graph = self.position_graphs.get(scene).copy()
            relevant_vertices = set(self.filter_vertices_from_graph(scene_graph, **filters))
            if allowed_products_filters:
                allowed_vertices = self.filter_vertices_from_graph(scene_graph, **allowed_products_filters)
            else:
                allowed_vertices = set()
            # other_vertices = {v.index for v in scene_graph.vs.select(product_type='Other')}
            if include_empty == self.EXCLUDE_EMPTY:
                empty_vertices = {v.index for v in scene_graph.vs.select(product_type='Empty')}
                allowed_vertices = set(allowed_vertices).union(empty_vertices)
            if include_other == self.EXCLUDE_OTHER:
                other_vertices = {v.index for v in scene_graph.vs.select(product_type='Other')}
                allowed_vertices = set(allowed_vertices).union(other_vertices)
            if include_private_label:
                p_l_vertices = {v.index for v in scene_graph.vs.select(PRIVATE_LABEL='Y')}
                allowed_vertices = set(allowed_vertices).union(p_l_vertices)
            # if include_irrelevant == self.EXCLUDE_IRRELEVANT:
            #     empty_vertices = {v.index for v in scene_graph.vs.select(product_type='Irrelevant')}
            #     allowed_vertices = set(allowed_vertices).union(empty_vertices)

            all_vertices = {v.index for v in scene_graph.vs}
            vertices_to_remove = all_vertices.difference(relevant_vertices.union(allowed_vertices))
            scene_graph.delete_vertices(list(vertices_to_remove))
            clusters = [cluster for cluster in scene_graph.clusters() if set(cluster).difference(allowed_vertices)]
            if group:
                new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **block_products)
            elif block_of_blocks:
                new_relevant_vertices1 = self.filter_vertices_from_graph(scene_graph, **block_products1)
                new_relevant_vertices2 = self.filter_vertices_from_graph(scene_graph, **block_products2)
            else:
                new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **filters)
            for cluster in clusters:
                if not set(cluster).difference(allowed_vertices):
                    clusters.remove(cluster)

            for cluster in clusters:
                if block_of_blocks:
                    results = {}
                    results['block_of_blocks'] = False
                    relevant_vertices_in_cluster1 = set(cluster).intersection(new_relevant_vertices1)
                    if len(new_relevant_vertices1) > 0:
                        cluster_ratio1 = len(relevant_vertices_in_cluster1) / float(len(new_relevant_vertices1))
                    else:
                        cluster_ratio1 = 0
                    relevant_vertices_in_cluster2 = set(cluster).intersection(new_relevant_vertices2)
                    if len(new_relevant_vertices2) > 0:
                        cluster_ratio2 = len(relevant_vertices_in_cluster2) / float(len(new_relevant_vertices2))
                    else:
                        cluster_ratio2 = 0
                    if cluster_ratio1 >= minimum_block_ratio and cluster_ratio2 >= minimum_block_ratio:
                        results['block_of_blocks'] = True
                        return results
                else:
                    relevant_vertices_in_cluster = set(cluster).intersection(new_relevant_vertices)
                    if len(new_relevant_vertices) > 0:
                        cluster_ratio = len(relevant_vertices_in_cluster) / float(len(new_relevant_vertices))
                    else:
                        cluster_ratio = 0
                    cluster_ratios.append(cluster_ratio)
                    if cluster_ratio >= minimum_block_ratio:
                        results = {'regular block': True}
                        all_vertices = {v.index for v in scene_graph.vs}
                        non_cluster_vertices = all_vertices.difference(cluster)
                        block_graph = scene_graph
                        block_graph.delete_vertices(non_cluster_vertices)
                        if vertical:
                            results['Vertical Block'] = False
                            block_vertices = {v.index for v in block_graph.vs}
                            block_vertices_to_check = set(new_relevant_vertices) & set(block_vertices)
                            for vertex1 in block_vertices_to_check:
                                if results['Vertical Block']:
                                    break
                                for vertex2 in block_vertices:
                                    if results['Vertical Block']:
                                        break
                                    path_list = block_graph.get_all_shortest_paths(vertex1, vertex2)
                                    pass_count = 0
                                    for plist in path_list:
                                        if len(plist) == 3:
                                            pass_count += 1
                                            if pass_count == 2:
                                                results['Vertical Block'] = True
                                                break

                        if horizontal:
                            results['horizontally block'] = False
                            direction_data = {'left': (2, 2), 'right': (2, 2)}
                            result = self.calculate_relative_in_block_per_graph(scene_graph=block_graph,
                                                                                direction_data=direction_data)
                            results['horizontally block'] = result
                        # if orphan:
                        #     orphan_clusters = clusters
                        #     orphan_clusters.remove(cluster)
                        #     direction_data = {'top': (3, 1000), 'bottom': (3, 1000), 'left': (3, 1000),
                        #                       'right': (3, 1000)}
                        #     results['Orphan products'] = False
                        #     for orphan in orphan_clusters:
                        #         non_orphan = all_vertices.difference(cluster)
                        #         non_orphan_cluster_vertices = non_orphan.difference(orphan)
                        #         orphan_graph = self.position_graphs.get(scene).copy()
                        #         orphan_graph.delete_vertices(non_orphan_cluster_vertices)
                        #         if set(orphan) & set(new_relevant_vertices):
                        #             result = self.calculate_relative_in_block_per_graph(scene_graph=orphan_graph,
                        #                                                                 direction_data=direction_data,
                        #                                                                 sent_tested_vertices=cluster,
                        #                                                                 sent_anchor_vertices=orphan)
                        #             if result:
                        #                 results['Orphan products'] = result
                        #                 break
                        if group:
                            results['group'] = False
                            group_vertex = self.filter_vertices_from_graph(block_graph, **group_products)
                            group_clusters = [cluster for cluster in block_graph.clusters() if
                                              set(cluster).difference(group_vertex)]
                            direction_data = {'top': (3, 5), 'bottom': (3, 5), 'left': (3, 5),
                                              'right': (3, 5)}
                            for group_cluster in group_clusters:
                                result = self.calculate_relative_in_block_per_graph(block_graph, direction_data,
                                                                                    group_cluster, group_cluster)
                                results['group'] = result
                        if include_private_label:
                            if checkerboard:
                                results['checkerboarded'] = False
                                p_l_new_vertices = {v.index for v in block_graph.vs.select(PRIVATE_LABEL='Y')}
                                if set(p_l_new_vertices) & set(cluster):
                                    cluster_graph = block_graph
                                    non_cluster = set(new_relevant_vertices).difference(cluster)
                                    cluster_graph.delete_vertices(non_cluster)
                                    brands_list = {v['brand_name'] for v in block_graph.vs}
                                    results['checkerboarded'] = True
                                    results['brand_list'] = brands_list
                            else:
                                results['block_ign_plabel'] = True
                        if availability_param:
                            results['availability'] = False
                            attributes_list = {v[availability_param] for v in block_graph.vs}
                            if set(availability_value) & set(attributes_list):
                                results['availability'] = True
                        if color_wheel:
                            scene_match_fk_list = {v['scene_match_fk'] for v in block_graph.vs}
                            results['color_wheel'] = True
                            results['scene_match_fk'] = scene_match_fk_list

                        return results
        return False

    def calculate_block_together_new(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY,
                                     include_other=EXCLUDE_OTHER, include_irrelevant=EXCLUDE_IRRELEVANT,
                                     minimum_block_ratio=1, result_by_scene=False, vertical=False, horizontal=False,
                                     orphan=False, group=False, block_of_blocks=False, block_products1=None,
                                     block_products2=None,
                                     block_products=None, group_products=None, include_private_label=False,
                                     availability_param=None, availability_value=None, color_wheel=False,
                                     checkerboard=False, orch=False,**filters):
        """
        :param group_products: if we searching for group in block - this is the filter of the group inside the big block
        :param block_products: if we searching for group in block - this is the filter of the big block
        :param group: True if the kpi is for block of product inside a bigger block
        :param orphan: True if searching for orphan products 3 products away from the biggest block
        :param horizontal: True if the biggest block have to be at least 2X2
        :param vertical: True
        :param allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
        :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
        :param minimum_block_ratio: The minimum (block number of facings / total number of relevant facings) ratio
                                    in order for KPI to pass (if ratio=1, then only one block is allowed).
        :param result_by_scene: True - The result is a tuple of (number of passed scenes, total relevant scenes);
                                False - The result is True if at least one scene has a block, False - otherwise.
        :param filters: These are the parameters which the blocks are checked for.
        :return: see 'result_by_scene' above.
        """
        if block_products is None:
            block_products = {}
        if block_products1 is None:
            block_products1 = {}
        if block_products2 is None:
            block_products2 = {}
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            if result_by_scene:
                return 0, 0
            else:
                Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
                result = 'no_products'
                return result
        number_of_blocked_scenes = 0
        cluster_ratios = []
        horizontal_flag = vertical_flag = False
        for scene in relevant_scenes:
            scene_graph = self.position_graphs.get(scene).copy()
            relevant_vertices = set(self.filter_vertices_from_graph(scene_graph, **filters))
            if allowed_products_filters:
                allowed_vertices = self.filter_vertices_from_graph(scene_graph, **allowed_products_filters)
            else:
                allowed_vertices = set()
            # other_vertices = {v.index for v in scene_graph.vs.select(product_type='Other')}
            if include_empty == self.EXCLUDE_EMPTY:
                empty_vertices = {v.index for v in scene_graph.vs.select(product_type='Empty')}
                allowed_vertices = set(allowed_vertices).union(empty_vertices)
            if include_other == self.EXCLUDE_OTHER:
                other_vertices = {v.index for v in scene_graph.vs.select(product_type='Other')}
                allowed_vertices = set(allowed_vertices).union(other_vertices)
            if include_private_label:
                p_l_vertices = {v.index for v in scene_graph.vs.select(PRIVATE_LABEL='Y')}
                allowed_vertices = set(allowed_vertices).union(p_l_vertices)
            # if include_irrelevant == self.EXCLUDE_IRRELEVANT:
            #     empty_vertices = {v.index for v in scene_graph.vs.select(product_type='Irrelevant')}
            #     allowed_vertices = set(allowed_vertices).union(empty_vertices)

            all_vertices = {v.index for v in scene_graph.vs}
            vertices_to_remove = all_vertices.difference(relevant_vertices.union(allowed_vertices))
            scene_graph.delete_vertices(list(vertices_to_remove))
            clusters = [cluster for cluster in scene_graph.clusters() if set(cluster).difference(allowed_vertices)]
            if group:
                new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **block_products)
            elif block_of_blocks:
                new_relevant_vertices1 = self.filter_vertices_from_graph(scene_graph, **block_products1)
                new_relevant_vertices2 = self.filter_vertices_from_graph(scene_graph, **block_products2)
            else:
                new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **filters)
            for cluster in clusters:
                if not set(cluster).difference(allowed_vertices):
                    clusters.remove(cluster)

            for cluster in clusters:
                if block_of_blocks:
                    results = {}
                    results['block_of_blocks'] = False
                    relevant_vertices_in_cluster1 = set(cluster).intersection(new_relevant_vertices1)
                    if len(new_relevant_vertices1) > 0:
                        cluster_ratio1 = len(relevant_vertices_in_cluster1) / float(len(new_relevant_vertices1))
                    else:
                        cluster_ratio1 = 0
                    relevant_vertices_in_cluster2 = set(cluster).intersection(new_relevant_vertices2)
                    if len(new_relevant_vertices2) > 0:
                        cluster_ratio2 = len(relevant_vertices_in_cluster2) / float(len(new_relevant_vertices2))
                    else:
                        cluster_ratio2 = 0
                    if cluster_ratio1 >= minimum_block_ratio and cluster_ratio2 >= minimum_block_ratio:
                        results['block_of_blocks'] = True
                        results['regular block'] = True
                        return results
                else:
                    relevant_vertices_in_cluster = set(cluster).intersection(new_relevant_vertices)
                    if len(new_relevant_vertices) > 0:
                        cluster_ratio = len(relevant_vertices_in_cluster) / float(len(new_relevant_vertices))
                    else:
                        cluster_ratio = 0
                    cluster_ratios.append(cluster_ratio)
                    if cluster_ratio >= minimum_block_ratio:
                        results = {'regular block': True}
                        all_vertices = {v.index for v in scene_graph.vs}
                        non_cluster_vertices = all_vertices.difference(cluster)
                        block_graph = scene_graph
                        block_graph.delete_vertices(non_cluster_vertices)
                        if horizontal or vertical:
                            edges = self.get_block_edges_new(block_graph.vs)
                            y_value = edges['visual'].get('top') - edges['visual'].get('bottom')
                            x_value = edges['visual'].get('right') - edges['visual'].get('left')
                            results['vertical'] = False
                            results['horizontal'] = False
                            if y_value > x_value:
                                vertical_flag = True
                            else:
                                horizontal_flag = True
                            try:
                                orientation = self.handle_horizontal_and_vertical(block_graph.vs,
                                                                                  max(edges.get('shelves')))
                                if orientation == 'HORIZONTAL':
                                    horizontal_flag = True
                                    vertical_flag = False
                                elif orientation == 'VERTICAL':
                                    vertical_flag = True
                                    horizontal_flag = False
                                else:
                                    pass
                            except Exception as e:
                                # Log.info('Orientation fix failed due to {}'.format(e))
                                pass
                            results['vertical'] = vertical_flag
                            results['horizontal'] = horizontal_flag
                        if orch:
                            edges = self.get_block_edges_new(block_graph.vs, x_att='rect_x', y_att='rect_y')
                            block_height = edges['visual'].get('top') - edges['visual'].get('bottom')
                            avg_y = sum(block_graph.vs.get_attribute_values('rect_y')) / float(
                                len(block_graph.vs.get_attribute_values('rect_y')))
                            return block_height, avg_y
                        if include_private_label:
                            p_l_new_vertices = {v.index for v in block_graph.vs.select(PRIVATE_LABEL='Y')}
                            if checkerboard:
                                results['checkerboarded'] = False
                                direction_data = {'top': (1, 1000), 'bottom': (1, 1000), 'left': (1, 1000),
                                                  'right': (1, 1000)}
                                relative_position_res = \
                                    self.calculate_relative_in_block_per_graph(block_graph, direction_data,
                                                                               min_required_to_pass=2,
                                                                               sent_tested_vertices=cluster,
                                                                               sent_anchor_vertices=p_l_new_vertices)
                                if set(p_l_new_vertices) & set(cluster):
                                    cluster_graph = block_graph
                                    non_cluster = set(new_relevant_vertices).difference(cluster)
                                    cluster_graph.delete_vertices(non_cluster)
                                    brands_list = {v['brand_name'] for v in block_graph.vs}
                                    if relative_position_res:
                                        results['checkerboarded'] = True
                                    results['brand_list'] = brands_list
                            else:
                                if len(p_l_new_vertices) / float(len(cluster)) >= 0.5:
                                    results['block_ign_plabel'] = True
                        if availability_param:
                            results['availability'] = False
                            attributes_list = {v[availability_param] for v in block_graph.vs}
                            if set(availability_value) & set(attributes_list):
                                results['availability'] = True
                        if color_wheel:
                            scene_match_fk_list = {v['scene_match_fk'] for v in block_graph.vs}
                            results['color_wheel'] = True
                            results['scene_match_fk'] = scene_match_fk_list

                        return results
        if orch:
            return 0, 0
        return False

    def get_block_edges_new(self, graph, x_att=None, y_att=None):
        """
        This function receives one or more vertex data of a block's graph, and returns the range of its edges -
        The far most top, bottom, left and right pixels of its facings.
        """
        top = right = bottom = left = None
        if x_att is None:
            x_att = 'x_mm'
        if y_att is None:
            y_att = 'y_mm'

        top = graph.get_attribute_values(y_att)
        top_index = max(xrange(len(top)), key=top.__getitem__)
        top_height = graph.get_attribute_values('height_mm_advance')[top_index]
        top = graph.get_attribute_values(y_att)[top_index]
        top += top_height / 2

        bottom = graph.get_attribute_values(y_att)
        bottom_index = min(xrange(len(bottom)), key=bottom.__getitem__)
        bottom_height = graph.get_attribute_values('height_mm_advance')[bottom_index]
        bottom = graph.get_attribute_values(y_att)[bottom_index]
        bottom -= bottom_height / 2

        left = graph.get_attribute_values(x_att)
        left_index = min(xrange(len(left)), key=left.__getitem__)
        left_height = graph.get_attribute_values('width_mm_advance')[left_index]
        left = graph.get_attribute_values(x_att)[left_index]
        left -= left_height / 2

        right = graph.get_attribute_values(x_att)
        right_index = max(xrange(len(right)), key=right.__getitem__)
        right_width = graph.get_attribute_values('width_mm_advance')[right_index]
        right = graph.get_attribute_values(x_att)[right_index]
        right += right_width / 2

        result = {'visual': {'top': top, 'right': right, 'bottom': bottom, 'left': left}}
        result.update({'shelves': list(set(graph.get_attribute_values('shelf_number')))})
        return result

    def handle_horizontal_and_vertical(self, graph, num_of_shelves):
        x_values = sorted(graph.get_attribute_values('rect_x'))
        y_values = sorted(graph.get_attribute_values('rect_y'))
        max_x = max(x_values)
        min_x = min(x_values)
        max_y = max(y_values)
        min_y = min(y_values)
        if num_of_shelves:
            y_division_factor = (max_y - min_y) / float(num_of_shelves)
        else:
            return
        x_width_feet = ((max_x - min_x) * self.MM_TO_FEET_CONVERSION)
        # y_bins = [list(z) for z in np.array_split(y_values, y_division_factor)]
        y_bins = []
        current_min_y = min_y
        for i in xrange(num_of_shelves):
            n_bin = [y_value for y_value in y_values if
                     y_value in range(current_min_y, current_min_y + int(y_division_factor))]
            y_bins.append(n_bin)
            current_min_y += int(y_division_factor)
            if current_min_y >= max_y:
                break
            n_bin = []
        average_num_of_vertices_in_y_bins = sum([len(x) for x in y_bins]) / float(len([len(x) for x in y_bins]))
        updated_y_bins = self.handle_block_outliers(y_bins, average_num_of_vertices_in_y_bins)
        x_bins = []
        current_min_x = min_x * self.MM_TO_FEET_CONVERSION
        for z in range(int(x_width_feet) + 1):
            n_bin = [x_value for x_value in x_values if current_min_x <= x_value * self.MM_TO_FEET_CONVERSION <
                     current_min_x + 1]
            x_bins.append(n_bin)
            current_min_x += 1
            if current_min_x >= max_x * self.MM_TO_FEET_CONVERSION:
                break
            n_bin = []
        average_num_of_vertices_in_x_bins = sum([len(x) for x in x_bins]) / float(len([len(x) for x in x_bins]))
        updated_x_bins = self.handle_block_outliers(x_bins, average_num_of_vertices_in_x_bins)
        if updated_x_bins and updated_y_bins:
            if max(updated_x_bins[-1]) - min(updated_x_bins[0]) > max(updated_y_bins[-1]) - min(updated_y_bins[0]):
                return 'HORIZONTAL'
            else:
                return 'VERTICAL'
        else:
            return None



            # todo split into function and return filtered x and y values

    def handle_block_outliers(self, bins, average_count):
        for i in range(len(bins)):
            if len(bins[i]) < average_count:
                bins.pop(i)
            else:
                break
        for j in reversed(range((len(bins)))):
            if len(bins[j]) < average_count:
                bins.pop(j)
            else:
                break
        return bins

    def calculate_flexible_blocks(self, number_of_allowed_others=3, group=None, **filters):
        """
        :param number_of_allowed_others: Number of allowed irrelevant facings between two cluster of relevant facings.
        :param filters: The relevant facings of the block.
        :return: This function calculates the number of 'flexible blocks' per scene, meaning, blocks which are allowed
                 to have a given number of irrelevant facings between actual chunks of relevant facings.
        """
        results = {}
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
            return results
        for scene in relevant_scenes:
            scene_graph = self.position_graphs.get(scene).copy()
            blocks, scene_graph = self.get_scene_blocks(scene_graph, **filters)
            blocks.sort(key=lambda x: len(x), reverse=True)
            blocks = [(0, self.get_block_edges(scene_graph.copy().vs[block])) for block in blocks]
            new_blocks = self.merge_blocks_into_flexible_blocks(filters, scene, number_of_allowed_others, list(blocks))
            while len(blocks) != len(new_blocks):
                blocks = list(new_blocks)
                new_blocks = self.merge_blocks_into_flexible_blocks(filters, scene, number_of_allowed_others, blocks)
            if group:
                results[scene] = new_blocks

            else:
                results[scene] = len(new_blocks)
        return results

    def merge_blocks_into_flexible_blocks(self, filters, scene_id, number_of_allowed_others, blocks):
        """
        This function receives blocks' ranges and tries to merge them based on an allowed number of irrelevant facings
        between them. If it manages to merge two blocks, it removes the original blocks and adds the merged block,
        and returns the new list immediately (merges at most one pair of blocks in one run).
        """
        for block1 in blocks:
            previous1, range1 = block1
            for block2 in blocks:
                previous2, range2 = block2

                if block1 != block2:
                    top = min(range1[0], range2[0])
                    right = max(range1[1], range2[1])
                    bottom = max(range1[2], range2[2])
                    left = min(range1[3], range2[3])

                    number_of_others = self.get_number_of_others_in_block_range(filters, scene_id, top, right, bottom,
                                                                                left)
                    previous_others = previous1 + previous2

                    if number_of_others <= number_of_allowed_others + previous_others:
                        blocks.insert(0, (previous_others + number_of_others, (top, right, bottom, left)))
                        blocks.remove(block1)
                        blocks.remove(block2)
                        return blocks
        return blocks

    def get_block_edges(self, *block_graphs):
        """
        This function receives one or more vertex data of a block's graph, and returns the range of its edges -
        The far most top, bottom, left and right pixels of its facings.
        """
        top = right = bottom = left = None
        for graph in block_graphs:
            max_top = min(graph.get_attribute_values(self.position_graphs.TOP))
            max_right = max(graph.get_attribute_values(self.position_graphs.RIGHT))
            max_bottom = max(graph.get_attribute_values(self.position_graphs.BOTTOM))
            max_left = min(graph.get_attribute_values(self.position_graphs.LEFT))
            if top is None or max_top < top:
                top = max_top
            if right is None or max_right > right:
                right = max_right
            if bottom is None or max_bottom > bottom:
                bottom = max_bottom
            if left is None or max_left < left:
                left = max_left
        return top, right, bottom, left

    def get_number_of_others_in_block_range(self, filters, scene_id, top, right, bottom, left):
        """
        This function gets a scene, a range (in pixels) and filters, and checks how many facings are in that range
         and are not part of the filters.
        """
        matches = self.match_product_in_scene[(self.match_product_in_scene['scene_fk'] == scene_id) &
                                              (~self.match_product_in_scene['product_type'].isin(['Empty']))]
        facings_in_range = matches[((matches[self.position_graphs.TOP].between(top, bottom - 1)) |
                                    (matches[self.position_graphs.BOTTOM].between(top + 1, bottom))) &
                                   ((matches[self.position_graphs.LEFT].between(left, right - 1)) |
                                    (matches[self.position_graphs.RIGHT].between(left + 1, right)))]
        relevant_facings_in_range = facings_in_range[self.get_filter_condition(facings_in_range, **filters)]
        other_facings_in_range = len(facings_in_range) - len(relevant_facings_in_range)
        return other_facings_in_range

    def get_product_unique_position_on_shelf(self, scene_id, shelf_number, include_empty=False, **filters):
        """
        :param scene_id: The scene ID.
        :param shelf_number: The number of shelf in question (from top).
        :param include_empty: This dictates whether or not to include empties as valid positions.
        :param filters: These are the parameters which the unique position is checked for.
        :return: The position of the first SKU (from the given filters) to appear in the specific shelf.
        """
        shelf_matches = self.match_product_in_scene[(self.match_product_in_scene['scene_fk'] == scene_id) &
                                                    (self.match_product_in_scene['shelf_number'] == shelf_number)]
        if not include_empty:
            filters['product_type'] = ('Empty', self.EXCLUDE_FILTER)
        if filters and shelf_matches[self.get_filter_condition(shelf_matches, **filters)].empty:
            Log.info("Products of '{}' are not tagged in shelf number {}".format(filters, shelf_number))
            return None
        shelf_matches = shelf_matches.sort_values(by=['bay_number', 'facing_sequence_number'])
        shelf_matches = shelf_matches.drop_duplicates(subset=['product_ean_code'])
        positions = []
        for m in xrange(len(shelf_matches)):
            match = shelf_matches.iloc[m]
            match_name = 'Empty' if match['product_type'] == 'Empty' else match['product_ean_code']
            if positions and positions[-1] == match_name:
                continue
            positions.append(match_name)
        return positions

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUPNGAMERICAGENERALToolBox.INCLUDE_FILTER)
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

    def separate_location_filters_from_product_filters(self, **filters):
        """
        This function gets scene-item-facts filters of all kinds, extracts the relevant scenes by the location filters,
        and returns them along with the product filters only.
        """
        relevant_scenes = self.scif[self.get_filter_condition(self.scif, **filters)]['scene_id'].unique()
        location_filters = {}
        for field in filters.keys():
            if field not in self.all_products.columns and field in self.scif.columns:
                location_filters[field] = filters.pop(field)
        relevant_scenes = self.scif[self.get_filter_condition(self.scif, **location_filters)]['scene_id'].unique()
        return filters, relevant_scenes

    @staticmethod
    def get_json_data(file_path, sheet_name=None, skiprows=0):
        """
        This function gets a file's path and extract its content into a JSON.
        """
        data = {}
        if sheet_name:
            sheet_names = [sheet_name]
        else:
            sheet_names = xlrd.open_workbook(file_path).sheet_names()
        for sheet_name in sheet_names:
            try:
                output = pd.read_excel(file_path, sheetname=sheet_name, skiprows=skiprows)
            except xlrd.biffh.XLRDError:
                Log.warning('Sheet name {} doesn\'t exist'.format(sheet_name))
                return None
            output = output.to_json(orient='records')
            output = json.loads(output)
            for x in xrange(len(output)):
                for y in output[x].keys():
                    output[x][y] = unicode('' if output[x][y] is None else output[x][y]).strip()
                    if not output[x][y]:
                        output[x].pop(y, None)
            data[sheet_name] = output
        if sheet_name:
            data = data[sheet_name]
        elif len(data.keys()) == 1:
            data = data[data.keys()[0]]
        return data
