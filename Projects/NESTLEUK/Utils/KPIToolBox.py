import pandas as pd

from datetime import datetime
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Projects.NESTLEUK.Utils.ParseTemplates import NESTLEUKParseTemplates
from Projects.NESTLEUK.Utils.Fetcher import NESTLEUKQueries
from Projects.NESTLEUK.Utils.GeneralToolBox import NESTLEUKGENERALToolBox
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime


__author__ = 'uri'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result

        return wrapper

    return decorator


class NESTLEUKConsts(object):
    BINARY = 'BINARY'
    PROPORTIONAL = 'PROPORTIONAL'
    AVERAGE = 'NORMALIZED AVERAGE'

    FLEXIBLE = 'FLEXIBLE'
    STRICT = 'STRICT'

    KPI_NAME = 'KPI name Eng'
    MULTIPLE_SURVEYS = 'Multiple Surveys'
    SURVEY = 'Survey Question'
    SHARE_OF_SHELF = 'SOS Facings'
    BPPC = 'BPPC'
    SHELF_POSITION = 'Shelf Position'
    BLOCK_TOGETHER = 'Block Together'
    AVAILABILITY = 'Availability'
    FACING_COUNT = 'Facing Count'
    FACING_SOS = 'Facing SOS'
    SURVEY_AND_AVAILABILITY = BPPC

    VISIBLE = 'Visible'
    DIAMOND = 'Diamond'
    BOTTOM_SHELF = 'Bottom shelf'
    ADJACENT = 'Adjacent'
    PSERVICE_CUSTOM_SCIF = 'pservice.custom_scene_item_facts'


class NESTLEUKToolBox(NESTLEUKConsts):
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = NESTLEUKGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.store_type = self.store_info['store_type'].iloc[0]
        self.store_type = '' if self.store_type is None else self.store_type
        self.templates_class = NESTLEUKParseTemplates('Nestle_UK_v3.0')
        self.template_ava_class = NESTLEUKParseTemplates('Template')
        self.templates_data = self.templates_class.parse_template(sheet_name='KPIs')
        self.template_ava_data = self.template_ava_class.parse_template(sheet_name='Hierarchy')
        self.template_ava_visible = self.template_ava_class.parse_template(sheet_name='Visible')
        self.template_ava_bottom_shelf = self.template_ava_class.parse_template(sheet_name='Bottom shelf')
        self.template_ava_adjacent = self.template_ava_class.parse_template(sheet_name='Adjacent')
        self.template_ava_diamond = self.template_ava_class.parse_template(sheet_name='Diamond')
        self.scores = pd.DataFrame(columns=['ean_code', 'visible', 'ava'])
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.custom_scif_queries = []

        # self.templates_data = self.template.parse_kpi()

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = NESTLEUKQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    # def main_calculation(self, set_name, *args, **kwargs):
    #     """
    #     This function calculates the KPI results.
    #     """
    #
    #     if set_name in ('OSA',):
    #         set_score = self.check_on_shelf_availability(set_name)
    #         # self.check_on_shelf_availability_on_scene_level(set_name)
    #     elif set_name in ('Linear Share of Shelf vs. Target', 'Linear Share of Shelf'):
    #         set_score = self.custom_share_of_shelf(set_name)
    #     elif set_name in ('Shelf Level',):
    #         set_score = self.calculate_eye_level_availability(set_name)
    #     elif set_name in ('Product Blocking',):
    #         set_score = self.calculate_block_together_sets(set_name)
    #     elif set_name == 'Pallet Presence':
    #         set_score, pallet_score, half_pallet_score = self.calculate_pallet_presence()
    #     elif set_name == 'Share of Assortment':
    #         set_score = self.calculate_share_of_assortment()
    #         self.save_level2_and_level3(set_name, set_name, set_score)
    #     elif set_name == 'Shelf Impact Score':
    #         set_score = self.shelf_impact_score()
    #
    #     else:
    #         return
    #     set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
    #     self.write_to_db_result(set_fk, set_score, self.LEVEL1)
    #     return set_score

    def calculate_nestle_score(self, set_name):
        """
        This function calculates the KPI results.
        """
        set_scores = {}
        main_children = self.templates_data[self.templates_data[self.templates_class.KPI_GROUP] == set_name]
        for c in xrange(len(main_children)):
            main_child = main_children.iloc[c]
            children = self.templates_data[self.templates_data[self.templates_class.KPI_GROUP] ==
                                           main_child[self.templates_class.KPI_NAME]]
            scores = []
            for i in xrange(len(children)):
                child = children.iloc[i]
                kpi_type = child[self.templates_class.KPI_TYPE]
                if not self.store_type in child[self.templates_class.STORE_TYPE]:
                    continue
                if not set(child[self.templates_class.SCENE_TYPE].split(self.templates_class.SEPARATOR)) & set(
                        self.scif['template_name'].unique().tolist()):
                    continue
                if kpi_type == self.BLOCK_TOGETHER:
                    score = self.calculate_block_together_sets(child)
                elif kpi_type == self.FACING_COUNT:
                    score = self.calculate_facing_count(child)
                elif kpi_type == self.AVAILABILITY:
                    score = self.calculate_availability(child)
                elif kpi_type == self.FACING_SOS:
                    score = self.calculate_facing_sos(child)
                elif kpi_type == self.SHELF_POSITION:
                    score = self.calculate_shelf_position(child)
                else:
                    Log.warning("KPI of type '{}' is not supported".format(kpi_type))
                    continue
                if score is not None:
                    child_score_weight = child[self.templates_class.WEIGHT]
                    atomic_fk = self.get_atomic_fk(child)
                    self.write_to_db_result(atomic_fk, score, level=self.LEVEL3)
                    if isinstance(score, tuple):
                        score = score[0]
                    weighted_score = score * float(child_score_weight)
                    scores.append(weighted_score)

            if not scores:
                scores = [0]
            if scores:
                score_type = main_child[self.templates_class.SCORE]
                score_weight = float(main_child[self.templates_class.WEIGHT])
                if score_type == self.templates_class.SUM_OF_SCORES:
                    score = sum(scores)
                else:
                    score = 0
                kpi_name = main_child[self.templates_class.KPI_NAME]
                kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name]['kpi_fk'].values[0]
                # self.write_to_db_result(kpi_fk, score, level=self.LEVEL2)
                set_scores[kpi_fk] = (score_weight, score)
        # total_weight = sum([score[0] for score in set_scores.values()])
        for kpi_fk in set_scores.keys():
            self.write_to_db_result(kpi_fk, set_scores[kpi_fk][1], level=self.LEVEL2)
        # set_score = sum([score[0] * score[1] for score in set_scores.values()]) / total_weight
        set_score = round(sum([score[0] * score[1] for score in set_scores.values()]), 2)
        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
        self.write_to_db_result(set_fk, set_score, level=self.LEVEL1)

    def calculate_ava(self):
        """
        This function calculates the KPI results.
        """
        set_scores = {}
        for set_name in self.template_ava_data['Set Name'].unique().tolist():
            kpk = self.template_ava_data[self.template_ava_data['Set Name'] == set_name]['KPI Group'].unique().tolist()
            for main_kpi in kpk:
                atomics = self.template_ava_data[self.template_ava_data['KPI Group'] == main_kpi]
                for i in xrange(len(atomics)):
                    atomic = atomics.iloc[i]
                    if not set(atomic[self.templates_class.SCENE_TYPE].split(self.templates_class.SEPARATOR)) & set(
                            self.scif['template_name'].unique().tolist()):
                        continue
                    else:
                        templates = map(lambda x: x.strip(), atomic[self.templates_class.SCENE_TYPE].split(','))
                        scenes_to_check = self.scif[self.scif['template_name'].isin(templates)][
                            'scene_fk'].unique().tolist()
                    kpi_type = atomic[self.templates_class.KPI_TYPE]
                    if kpi_type == self.BOTTOM_SHELF:
                        params = self.template_ava_bottom_shelf[self.template_ava_bottom_shelf['KPI Name'] == atomic['KPI Name']].iloc[0]
                        self.calculate_bottom_shelf(params, scenes_to_check)
                    elif kpi_type == self.ADJACENT:
                        params = self.template_ava_adjacent[self.template_ava_adjacent['KPI Name'] == atomic['KPI Name']].iloc[0]
                        self.calculate_adjacent(params, scenes_to_check)
                    elif kpi_type == self.DIAMOND:
                        params = self.template_ava_diamond[self.template_ava_diamond['KPI Name'] == atomic['KPI Name']].iloc[0]
                        self.calculate_diamond(params, scenes_to_check)
                    else:
                        Log.warning("KPI of type '{}' is not supported".format(kpi_type))
                        continue

    def get_custom_query(self, scene_fk, product_fk, in_assortment_OSA=0, oos_osa=0, mha_in_assortment=0,
                         mha_oos=0, length_mm_custom=0):
        attributes = pd.DataFrame([(
            self.session_fk, scene_fk, product_fk, in_assortment_OSA, oos_osa, mha_in_assortment,
            mha_oos, length_mm_custom)],
            columns=['session_fk', 'scene_fk', 'product_fk', 'in_assortment_OSA', 'oos_osa',
                     'mha_in_assortment', 'mha_oos', 'length_mm_custom'])

        query = insert(attributes.to_dict(), self.PSERVICE_CUSTOM_SCIF)
        self.custom_scif_queries.append(query)

    def calculate_bottom_shelf(self, kpi, scenes_to_check):
        target = int(kpi[self.templates_class.TARGET])
        shelf_number = map(lambda x: x.strip(), kpi['shelf_number_from_bottom'].split(','))
        shelf_percent = int(kpi['shelf_percent'])
        products_for_check = map(lambda x: x.strip(), kpi['product_ean_code'].split(','))
        products_for_check = self.all_products[self.all_products['product_ean_code'].isin(products_for_check)]['product_fk'].tolist()
        for scene in scenes_to_check:
            shelf_edges = self.build_shelf_edges(scene, shelf_percent)
            for product_fk in products_for_check:
                result = self.tools.calculate_availability(product_fk=product_fk, scene_fk=scene)
                if result:
                    in_assortment_osa = 1
                    result = self.calculate_contain(scene, product_fk, shelf_edges, shelf_number)
                    mha_in_assortment = 1 if result >= target else 0
                else:
                    in_assortment_osa = mha_in_assortment = 0
                self.get_custom_query(scene_fk=scene, product_fk=product_fk,
                                      in_assortment_OSA=in_assortment_osa, mha_in_assortment=mha_in_assortment)

    def build_shelf_edges(self, scene_fk, shelf_percent):
        matches = self.match_product_in_scene[self.tools.get_filter_condition(self.match_product_in_scene,
                                                                              **{'scene_fk': scene_fk})]
        left = matches.copy().sort_values('x_mm', ascending=True).iloc[0]
        left = int(left['x_mm']) - (int(left['width_mm']) / 2) # TODO width_mm_net
        right = matches.copy().sort_values('x_mm', ascending=False).iloc[0]
        right = int(right['x_mm']) + (int(right['width_mm']) / 2) # TODO width_mm_net
        shelf_len = right - left
        shelf_len_after_downsize = (shelf_len - (shelf_len * shelf_percent / 100)) / 2
        edges = {'left': left + shelf_len_after_downsize, 'right': right - shelf_len_after_downsize}
        return edges

    def build_product_edges(self, matches):
        points = []
        for x, product_show in matches.iterrows():
            left = int(product_show['x_mm']) - (int(product_show['width_mm']) / 2)  # TODO width_mm_net
            right = int(product_show['x_mm']) + (int(product_show['width_mm']) / 2)  # TODO width_mm_net
            edges_point = {'left': left, 'right': right}
            points.append(edges_point)
        return points

    def calculate_contain(self, scene, product_fk, shelf_edges, shelf_number):
        matches = self.match_product_in_scene[self.tools.get_filter_condition(self.match_product_in_scene,
                                                                              **{'scene_fk': scene,
                                                                                 'shelf_number_from_bottom': shelf_number,
                                                                                 'product_fk': product_fk})]
        points = self.build_product_edges(matches)
        for point in points:
            if (shelf_edges['left'] < point['left'] < shelf_edges['right']) or \
                    (shelf_edges['left'] < point['right'] < shelf_edges['right']):
                return True
        return False

    def calculate_diamond(self, kpi, scenes_to_check):
        target = int(kpi[self.templates_class.TARGET])
        products_for_check = map(lambda x: x.strip(), kpi['product_ean_code'].split(','))
        products_for_check = self.all_products[self.all_products['product_ean_code'].isin(products_for_check)]['product_fk'].tolist()
        for scene in scenes_to_check:
            if self.validate_scene(scene):
                polygon = self.build_diamond_polygon(scene)
                for product_fk in products_for_check:
                    result = self.tools.calculate_availability(product_fk=product_fk, scene_fk=scenes_to_check)
                    if result:
                        in_assortment_osa = 1
                        result = self.calculate_polygon(scene=scene, product_fk=product_fk, polygon=polygon)
                        mha_in_assortment = 1 if result >= target else 0
                    else:
                        in_assortment_osa = mha_in_assortment = 0
                    self.get_custom_query(scene_fk=scene, product_fk=product_fk,
                                          in_assortment_OSA=in_assortment_osa, mha_in_assortment=mha_in_assortment)

    def validate_scene(self, scene_fk):
        matches = self.match_product_in_scene[self.tools.get_filter_condition(self.match_product_in_scene,
                                                                              **{'scene_fk': scene_fk})]
        if len(matches['shelf_number'].unique().tolist()) > 1:
            return True
        return False

    def build_diamond_polygon(self, scene_fk):
        matches = self.match_product_in_scene[self.tools.get_filter_condition(self.match_product_in_scene,
                                                                              **{'scene_fk': scene_fk})]
        shelf_number = min(matches['shelf_number'].unique().tolist())
        top = matches[(matches['shelf_number'] == shelf_number) &
                  (matches['stacking_layer'] == 1)].sort_values('y_mm', ascending=False).iloc[0]
        top = int(top['y_mm']) - (int(top['height_mm']) / 2) # TODO height_mm_net
        try:
            bottom = matches[(matches['shelf_number_from_bottom'] == 2) &
                             (matches['stacking_layer'] == 1)].sort_values('y_mm', ascending=False).iloc[0]
            bottom = int(bottom['y_mm']) - (int(bottom['height_mm']) / 2) # TODO height_mm_net
        except:
            bottom = matches[matches['shelf_number_from_bottom'] == 1].sort_values('y_mm', ascending=False).iloc[0]
            bottom = int(bottom['y_mm']) + (int(bottom['height_mm']) / 2)  # TODO height_mm_net
        left = matches.copy().sort_values('x_mm', ascending=True).iloc[0]
        left = int(left['x_mm']) - (int(left['width_mm']) / 2) # TODO width_mm_net
        right = matches.copy().sort_values('x_mm', ascending=False).iloc[0]
        right = int(right['x_mm']) + (int(right['width_mm']) / 2) # TODO width_mm_net
        middle_x = (right + left) / 2
        middle_y = (top + bottom) / 2
        polygon = Polygon([(middle_x, top), (right, middle_y), (middle_x, bottom), (left, middle_y)])
        return polygon

    def calculate_polygon(self, scene, product_fk, polygon):
        matches = self.match_product_in_scene[self.tools.get_filter_condition(self.match_product_in_scene, **{'scene_fk': scene})]
        points = self.build_array_of_points(matches, product_fk)
        for point in points:
            if polygon.contains(point):
                return True
        return False

    def build_array_of_points(self, matches, product):
        points = []
        for x, product_show in matches[matches['product_fk'] == product].iterrows():
            top = int(product_show['y_mm']) + (int(product_show['height_mm']) / 2)  # TODO height_mm_net
            bottom = int(product_show['y_mm']) - (int(product_show['height_mm']) / 2)  # TODO height_mm_net
            left = int(product_show['x_mm']) - (int(product_show['width_mm']) / 2)  # TODO width_mm_net
            right = int(product_show['x_mm']) + (int(product_show['width_mm']) / 2)  # TODO width_mm_net
            mask_point = Point(left, top), Point(right, top), Point(left, bottom), Point(right, bottom)
            points += mask_point
        return points

    def calculate_adjacent(self, kpi, scenes_to_check):
        adjacent_type = kpi['adjacent_type']
        adjacent_value = kpi['adjacent_value']
        anchor_filters = {adjacent_type: adjacent_value}
        products_for_check = map(lambda x: x.strip(), kpi['product_ean_code'].split(','))
        products_for_check = self.all_products[self.all_products['product_ean_code'].isin(products_for_check)]['product_fk'].tolist()
        general_filters = {'scene_fk': scenes_to_check}
        for scene in scenes_to_check:
            for product_fk in products_for_check:
                result = self.tools.calculate_availability(product_fk=product_fk, scene_fk=scenes_to_check)
                if result:
                    in_assortment_osa = 1
                    result = not self.tools.calculate_non_proximity(tested_filters={'product_fk': product_fk},
                                                                    anchor_filters=anchor_filters,
                                                                    allowed_diagonal=False,
                                                                    **general_filters)
                    mha_in_assortment = 1 if result else 0
                else:
                    in_assortment_osa = mha_in_assortment = 0
                self.get_custom_query(scene_fk=scene, product_fk=product_fk,
                                      in_assortment_OSA=in_assortment_osa, mha_in_assortment=mha_in_assortment)

    @kpi_runtime()
    def calculate_block_together_sets(self, kpi):
        """
        This function calculates every block-together-typed KPI from the relevant sets, and returns the set final score.
        """
        templates = kpi[self.templates_class.SCENE_TYPE].split(self.templates_class.SEPARATOR)
        brands_for_block_check = kpi[self.templates_class.BRAND].split(self.templates_class.SEPARATOR)
        scenes_to_check = self.scif[self.scif['template_name'].isin(templates)]['scene_fk'].unique().tolist()
        if not kpi[self.templates_class.CATEGORY]:
            result = self.tools.calculate_block_together(brand_name=brands_for_block_check,
                                                         scene_fk=scenes_to_check)
        else:
            category = kpi[self.templates_class.CATEGORY]
            result = self.tools.calculate_block_together(brand_name=brands_for_block_check,
                                                         scene_fk=scenes_to_check,
                                                         category=category)
        score = 100 if result else 0

        return score

    @kpi_runtime()
    def calculate_facing_count(self, kpi):
        """
        This function calculates every block-together-typed KPI from the relevant sets, and returns the set final score.
        """
        templates = kpi[self.templates_class.SCENE_TYPE].split(self.templates_class.SEPARATOR)
        products_for_check = kpi[self.templates_class.SKU]
        scenes_to_check = self.scif[self.scif['template_name'].isin(templates)]['scene_fk'].unique().tolist()
        result = self.tools.calculate_availability(product_ean_code=products_for_check,
                                                   scene_fk=scenes_to_check,
                                                   stacking_layer=1)
        if kpi[self.templates_class.TARGET]:
            target = float(kpi[self.templates_class.TARGET])
        else:
            target = kpi[self.templates_class.TARGET]
        score = 100 if result >= target else 0

        return score

    @kpi_runtime()
    def calculate_availability(self, kpi):
        """
        This function calculates every block-together-typed KPI from the relevant sets, and returns the set final score.
        """
        kpi_name = kpi[self.templates_class.KPI_NAME]
        templates_data = self.templates_class.parse_template(sheet_name='Availability', lower_headers_row_index=4,
                                                             upper_headers_row_index=3, data_content_column_index=6,
                                                             input_column_name_separator=', ')
        scene_types = []
        scores = []
        session_templates = self.scif['template_name'].unique().tolist()
        for scene_type in session_templates:
            availability_id = '{};{}'.format(self.store_type, scene_type)
            if availability_id in templates_data.columns:
                availability_data = templates_data[(templates_data[self.templates_class.KPI_NAME] == kpi_name) &
                                                   (templates_data[availability_id] == 1)]
            else:
                continue
            if not availability_data.empty:
                scene_types.append(scene_type)
        products_for_check = templates_data[self.templates_class.availability_consts.PRODUCT_EAN_CODES].tolist()
        for products_list in products_for_check:
            try:
                products = products_list.split(', ')
            except Exception as e:
                products = products_list
            result = self.tools.calculate_availability(product_ean_code=products,
                                                       template_name=scene_types,
                                                       stacking_layer=1)
            score = 100 if result > 0 else 0
            scores.append(score)

        if 0 in scores:
            final_score = 0
        else:
            final_score = 100

        return final_score

    @kpi_runtime()
    def calculate_shelf_position(self, kpi):
        """
        This function calculates every block-together-typed KPI from the relevant sets, and returns the set final score.
        """
        kpi_name = kpi[self.templates_class.KPI_NAME]
        templates_data = self.templates_class.parse_template(sheet_name='Shelf Position')
        scores = []
        shelf_position_data = templates_data[(templates_data[self.templates_class.KPI_NAME] == kpi_name)]
        products_for_check = shelf_position_data[self.templates_class.availability_consts.PRODUCT_EAN_CODES].tolist()
        templates = kpi[self.templates_class.SCENE_TYPE].split(self.templates_class.SEPARATOR)
        scenes_to_check = self.scif[self.scif['template_name'].isin(templates)]['scene_fk'].unique().tolist()
        for products_list in products_for_check:
            try:
                products = products_list.split(', ')
            except Exception as e:
                products = products_list
            shelves = shelf_position_data.loc[
                shelf_position_data[self.templates_class.availability_consts.PRODUCT_EAN_CODES] == products_list][
                'Shelf Position'].values[0].split(',')
            result = self.tools.calculate_shelf_level_assortment(shelves=[int(shelf) for shelf in shelves],
                                                                 product_ean_code=products, scene_fk=scenes_to_check)
            score = 100 if result > 0 else 0
            scores.append(score)

        if 0 in scores:
            final_score = 0
        else:
            final_score = 100

        return final_score

    @kpi_runtime()
    def calculate_facing_sos(self, kpi):
        """
        This function calculates every block-together-typed KPI from the relevant sets, and returns the set final score.
        """
        templates = kpi[self.templates_class.SCENE_TYPE].split(self.templates_class.SEPARATOR)
        manufactruers_for_check = kpi[self.templates_class.MANUFACTURER]
        scenes_to_check = self.scif[self.scif['template_name'].isin(templates)]['scene_fk'].unique().tolist()
        if kpi[self.templates_class.CATEGORY] is None:
            sos_filters = {'manufacturer_name': manufactruers_for_check}
            result = self.tools.calculate_share_of_shelf(sos_filters=sos_filters,
                                                         scene_fk=scenes_to_check,
                                                         stacking_layer=1)
        else:
            sos_filters = {'manufacturer_name': manufactruers_for_check}
            category = kpi[self.templates_class.CATEGORY]
            result = self.tools.calculate_share_of_shelf(sos_filters=sos_filters,
                                                         scene_fk=scenes_to_check,
                                                         category=category,
                                                         stacking_layer=1)

        score = 100 if result > kpi[self.templates_class.TARGET] else 0

        return score

    def get_atomic_fk(self, params):
        """
        This function gets an Atomic KPI's FK out of the template data.
        """
        atomic_name = params[self.templates_class.KPI_NAME]
        kpi_name = params[self.templates_class.KPI_GROUP]
        atomic_fk = self.kpi_static_data[(self.kpi_static_data['kpi_name'] == kpi_name) &
                                         (self.kpi_static_data['atomic_kpi_name'] == atomic_name)]['atomic_kpi_fk']
        if atomic_fk.empty:
            return None
        return atomic_fk.values[0]

    def write_to_db_result(self, fk, score, level):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, score, level)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL2:
            table = KPK_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, fk, score, level):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    def commit_custom_scif(self):
        if not self.rds_conn.is_connected:
            self.rds_conn.connect_rds()
        cur = self.rds_conn.db.cursor()
        delete_query = NESTLEUKQueries.get_delete_session_custom_scif(self.session_fk)
        cur.execute(delete_query)
        self.rds_conn.db.commit()
        queries = self.merge_insert_queries(self.custom_scif_queries)
        for query in queries:
            try:
                cur.execute(query)
            except:
                print 'could not run query: {}'.format(query)
        self.rds_conn.db.commit()

    def merge_insert_queries(self, insert_queries):
        # other_queries = []
        query_groups = {}
        for query in insert_queries:
            if 'update' in query:
                self.update_queries.append(query)
            else:
                static_data, inserted_data = query.split('VALUES ')
                if static_data not in query_groups:
                    query_groups[static_data] = []
                query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            for group_index in xrange(0, len(query_groups[group]), 10 ** 4):
                merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group]
                                                                                [group_index:group_index + 10 ** 4])))
        # merged_queries.extend(other_queries)
        return merged_queries

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        self.commit_custom_scif()
        cur = self.rds_conn.db.cursor()
        delete_queries = NESTLEUKQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        queries = self.merge_insert_queries(self.kpi_results_queries)
        for query in queries:
            cur.execute(query)
        self.rds_conn.db.commit()
