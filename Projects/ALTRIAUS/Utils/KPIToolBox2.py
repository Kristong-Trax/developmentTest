import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
import os
from Trax.Utils.Logging.Logger import Log
import datetime
from Projects.ALTRIAUS.Utils.AltriaDataProvider import AltriaDataProvider
from Projects.ALTRIAUS.Utils.AltriaGraphUtil import AltriaGraphBuilder
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from shapely.geometry import box


__author__ = 'huntery'

FIXTURE_WIDTH_TEMPLATE = \
    os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Fixture_Width_Dimensions_v3.xlsx')

NO_FLIP_SIGN_PK = 11161

MATCH_PRODUCT_IN_PROBE_FK = 'match_product_in_probe_fk'
MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK = 'match_product_in_probe_state_reporting_fk'


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result

        return wrapper

    return decorator

REQUIRED_FLIP_SIGN_FSLOT_OVERLAP_RATIO = 0.3


class ALTRIAUSToolBox:
    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.common_v2 = CommonV2(self.data_provider)
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
        self.template_info = self.data_provider.all_templates
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.ps_data_provider = PsDataProvider(self.data_provider)
        self.match_product_in_probe_state_reporting = self.ps_data_provider.get_match_product_in_probe_state_reporting()
        self.kpi_results_queries = []
        self.fixture_width_template = pd.read_excel(FIXTURE_WIDTH_TEMPLATE, "Fixture Width", dtype=pd.Int64Dtype())
        self.facings_to_feet_template = pd.read_excel(FIXTURE_WIDTH_TEMPLATE, "Conversion Table", dtype=pd.Int64Dtype())
        self.header_positions_template = pd.read_excel(FIXTURE_WIDTH_TEMPLATE, "Header Positions")
        self.flip_sign_positions_template = pd.read_excel(FIXTURE_WIDTH_TEMPLATE, "Flip Sign Positions")
        self.custom_entity_data = self.ps_data_provider.get_custom_entities(1005)
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.kpi_new_static_data = self.common.get_new_kpi_static_data()
        try:
            self.mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
                        .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
                          .merge(self.template_info, on='template_fk', suffixes=['', '_t'])
        except KeyError:
            Log.warning('MPIS cannot be generated!')
            return
        self.adp = AltriaDataProvider(self.data_provider)
        self.active_kpis = self._get_active_kpis()
        self.external_targets = self.ps_data_provider.get_kpi_external_targets()
        self._add_smart_attributes_to_mpis()
        self.scene_graphs = {}

    def _add_smart_attributes_to_mpis(self):
        smart_attribute_data = \
            self.adp.get_match_product_in_probe_state_values(self.mpis['probe_match_fk'].unique().tolist())

        self.mpis = pd.merge(self.mpis, smart_attribute_data, on='probe_match_fk', how='left')
        self.mpis['match_product_in_probe_state_fk'].fillna(0, inplace=True)

    def main_calculation(self, *args, **kwargs):
        """
               This function calculates the KPI results.
               """
        self.generate_graph_per_scene()
        for graph in self.scene_graphs.values():
            self.calculate_fixture_and_block_level_kpis(graph)

        self.calculate_register_type()
        self.calculate_age_verification()
        self.calculate_facings_by_scene_type()

        return

    def calculate_fixture_and_block_level_kpis(self, graph):
        for node, node_data in graph.nodes(data=True):
            if node_data['category'].value != 'POS':
                self.calculate_fixture_width(node_data)
                self.calculate_flip_sign(node, node_data, graph)
                self.calculate_flip_sign_empty_space(node, node_data, graph)
                self.calculate_flip_sign_locations(node_data)
                self.calculate_total_shelves(node_data)
                self.calculate_tags_by_fixture_block(node_data)
                if node_data['block_number'].value == 1:
                    self.calculate_header(node, node_data, graph)
                    self.calculate_product_above_headers(node_data)
                    self.calculate_no_headers(node_data)
        return

    def generate_graph_per_scene(self):
        relevant_scif = self.scif[self.scif['template_name'] == 'Tobacco Merchandising Space']
        for scene in relevant_scif['scene_id'].unique().tolist():
            agb = AltriaGraphBuilder(self.data_provider, scene)
            self.scene_graphs[scene] = agb.get_graph()
        if len(self.scene_graphs.keys()) > 1:
            Log.warning("More than one Tobacco Merchandising Space scene detected. Results could be mixed!")
        return

    def calculate_fixture_width(self, node_data):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Fixture Width')

        width = node_data['calculated_width_ft'].value
        # width = round(width)
        fixture_number = node_data['fixture_number'].value
        block_number = node_data['block_number'].value
        category_fk = self.get_category_fk_by_name(node_data['category'].value)

        self.common_v2.write_to_db_result(kpi_fk, numerator_id=category_fk, denominator_id=self.store_id,
                                          numerator_result=block_number, denominator_result=fixture_number,
                                          result=width)

    def calculate_flip_sign(self, node, node_data, graph):
        if node_data['category'].value == 'Cigarettes':
            self.calculate_flip_signs_cigarettes(node, node_data, graph)
        else:
            self.calculate_flip_signs_non_cigarettes(node, node_data, graph)
        return

    def calculate_flip_signs_non_cigarettes(self, node, node_data, graph):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Flip Sign')
        fixture_number = node_data['fixture_number'].value
        block_number = node_data['block_number'].value

        flip_signs_by_x_coord = {}

        for neighbor in graph.neighbors(node):
            neighbor_data = graph.nodes[neighbor]
            if neighbor_data['category'].value != 'POS':
                continue
            if neighbor_data['pos_type'].value != 'Flip-Sign':
                continue

            center_x = neighbor_data['polygon'].centroid.coords[0][0]

            flip_signs_by_x_coord[center_x] = neighbor_data

        for i, pair in enumerate(sorted(flip_signs_by_x_coord.items())):
            position_fk = self.get_custom_entity_pk(str(i+1))
            product_fk = pair[1]['product_fk'].value
            width = pair[1]['calculated_width_ft'].value
            implied_facings = pair[1]['width_of_signage_in_facings'].value
            # width = round(width)

            self.common_v2.write_to_db_result(kpi_fk, numerator_id=product_fk, denominator_id=position_fk,
                                              numerator_result=block_number, denominator_result=fixture_number,
                                              result=width, score=implied_facings)

    def calculate_flip_signs_cigarettes(self, node, node_data, graph):
        width = node_data['calculated_width_ft'].value
        fixture_bounds = node_data['polygon'].bounds
        fixture_width_coord_units = abs(fixture_bounds[0] - fixture_bounds[2])
        proportions_dict = self.get_flip_sign_position_proportions(round(width))
        if not proportions_dict:
            return

        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Flip Sign')
        fixture_number = node_data['fixture_number'].value
        block_number = node_data['block_number'].value

        f_slot_boxes = {}
        left_bound = fixture_bounds[0]  # start proportional divisions on left side of fixture
        for position, proportion in proportions_dict.items():
            right_bound = left_bound + (fixture_width_coord_units * proportion)
            # TODO update this to use min and max height of graph
            f_slot_boxes[position] = box(left_bound, -9999, right_bound, 9999)
            left_bound = right_bound

        for position, slot_box in f_slot_boxes.items():
            for neighbor in graph.neighbors(node):
                neighbor_data = graph.nodes[neighbor]
                try:
                    if neighbor_data['pos_type'] != 'Flip-Sign':
                        continue
                except KeyError:
                    continue
                flip_sign_bounds = neighbor_data['polygon'].bounds
                flip_sign_box = box(flip_sign_bounds[0], flip_sign_bounds[1], flip_sign_bounds[2], flip_sign_bounds[3])

                overlap_ratio = flip_sign_box.intersection(slot_box).area / flip_sign_box.area
                if overlap_ratio >= REQUIRED_FLIP_SIGN_FSLOT_OVERLAP_RATIO:
                    product_fk = neighbor_data['product_fk'].value
                    position_fk = self.get_custom_entity_pk(position)
                    flip_sign_width = neighbor_data['calculated_width_ft'].value
                    implied_facings = neighbor_data['width_of_signage_in_facings'].value
                    self.common_v2.write_to_db_result(kpi_fk, numerator_id=product_fk, denominator_id=position_fk,
                                                      numerator_result=block_number, denominator_result=fixture_number,
                                                      result=flip_sign_width, score=implied_facings)
        return

    def calculate_flip_sign_empty_space(self, node, node_data, graph):
        if node_data['category'].value != 'Cigarettes':
            return

        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Empty Flip Sign Space')
        fixture_number = node_data['fixture_number'].value
        block_number = node_data['block_number'].value

        fixture_width = node_data['calculated_width_ft'].value
        # fixture_width = round(fixture_width)

        flip_sign_widths = []

        for neighbor in graph.neighbors(node):
            neighbor_data = graph.nodes[neighbor]
            if neighbor_data['category'].value != 'POS':
                continue
            if neighbor_data['pos_type'].value != 'Flip-Sign':
                continue
            # TODO exclude non Altria POS

            neighbor_width = neighbor_data['calculated_width_ft'].value
            flip_sign_widths.append(neighbor_width)

        empty_space = abs(fixture_width - sum(flip_sign_widths))

        self.common_v2.write_to_db_result(kpi_fk, numerator_id=49, denominator_id=self.store_id,
                                          numerator_result=block_number, denominator_result=fixture_number,
                                          result=empty_space)

    def calculate_flip_sign_locations(self, node_data):
        if node_data['category'].value != 'Cigarettes':
            return

        fixture_number = node_data['fixture_number'].value
        block_number = node_data['block_number'].value

        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Flip Sign Locations')

        width = node_data['calculated_width_ft'].value
        width = round(width)

        proportions_dict = self.get_flip_sign_position_proportions(width)
        if not proportions_dict:
            return

        for position, proportion in proportions_dict.items():
            position_fk = self.get_custom_entity_pk(position)
            self.common_v2.write_to_db_result(kpi_fk, numerator_id=49, denominator_id=position_fk,
                                              numerator_result=block_number, denominator_result=fixture_number,
                                              result=round(proportion * width))

        return

    def get_flip_sign_position_proportions(self, width):
        relevant_template = self.fixture_width_template[self.fixture_width_template['Fixture Width (ft)'] == width]
        if relevant_template.empty:
            Log.error("Unable to save flip sign location data. {}ft does not exist as a defined width".format(width))
            return None

        # this could definitely be simpler, but I'm tired and don't want to use my brain
        relevant_template[['F1', 'F2', 'F3', 'F4']] = relevant_template[['F1', 'F2', 'F3', 'F4']].fillna(0)

        f_slots = [relevant_template['F1'].iloc[0], relevant_template['F2'].iloc[0],
                   relevant_template['F3'].iloc[0], relevant_template['F4'].iloc[0]]

        f_slots = [i for i in f_slots if i != 0]

        proportions_dict = {}

        for i, slot in enumerate(f_slots):
            proportions_dict["F{}".format(i+1)] = slot / sum(f_slots)

        return proportions_dict

    def calculate_total_shelves(self, node_data):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Total Shelves')

        shelves = len(node_data['shelf_number'].values)
        fixture_number = node_data['fixture_number'].value
        block_number = node_data['block_number'].value
        category_fk = self.get_category_fk_by_name(node_data['category'].value)

        self.common_v2.write_to_db_result(kpi_fk, numerator_id=category_fk, denominator_id=self.store_id,
                                          numerator_result=block_number, denominator_result=fixture_number,
                                          result=shelves)

    def calculate_tags_by_fixture_block(self, node_data):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('FACINGS_BY_FIXTURE_BLOCK')

        scene_match_fks = node_data['match_fk'].values
        fixture_number = node_data['fixture_number'].value
        block_number = node_data['block_number'].value

        relevant_matches = self.mpis[self.mpis['scene_match_fk'].isin(scene_match_fks)]

        relevant_matches = relevant_matches.groupby(['product_fk', 'template_fk', 'match_product_in_probe_state_fk'],
                                                    as_index=False)['scene_match_fk'].count()
        relevant_matches.rename(columns={'scene_match_fk': 'facings'}, inplace=True)

        for row in relevant_matches.itertuples():
            self.common_v2.write_to_db_result(kpi_fk, numerator_id=row.product_fk, denominator_id=row.template_fk,
                                              numerator_result=block_number, denominator_result=fixture_number,
                                              result=row.facings, context_id=row.match_product_in_probe_state_fk)

    def calculate_header(self, node, node_data, graph):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Header')
        parent_category = node_data['category'].value
        fixture_number = node_data['fixture_number'].value
        block_number = node_data['block_number'].value

        if parent_category not in self.header_positions_template.columns.tolist():
            return
        headers_by_x_coord = {}
        menu_board_items_by_x_coord = {}

        for neighbor in graph.neighbors(node):
            neighbor_data = graph.nodes[neighbor]
            if neighbor_data['category'].value != 'POS':
                continue
            if neighbor_data['pos_type'].value != 'Header':
                continue

            center_x = neighbor_data['polygon'].centroid.coords[0][0]

            if neighbor_data['in_menu_board_area'].value is True:
                menu_board_items_by_x_coord[center_x] = neighbor_data
            else:
                headers_by_x_coord[center_x] = neighbor_data

        number_of_headers = len(headers_by_x_coord.keys())
        if menu_board_items_by_x_coord:
            number_of_headers += 1

        relevant_positions = \
            self.header_positions_template[(self.header_positions_template['Number of Headers'] == number_of_headers)]

        if relevant_positions.empty:
            if number_of_headers == 0:
                return
            else:
                Log.error("Too many headers ({}) found for one block ({}). Unable to calculate positions!".format(
                    number_of_headers, parent_category))
                return

        positions = relevant_positions[parent_category].iloc[0]
        positions = [position.strip() for position in positions.split(',')]

        for i, pair in enumerate(sorted(headers_by_x_coord.items())):
            position_fk = self.get_custom_entity_pk(positions[i])
            product_fk = pair[1]['product_fk'].value
            width = pair[1]['calculated_width_ft'].value
            # width = round(width)

            self.common_v2.write_to_db_result(kpi_fk, numerator_id=product_fk, denominator_id=position_fk,
                                              numerator_result=block_number, denominator_result=fixture_number,
                                              result=width, score=width)

        if menu_board_items_by_x_coord:
            for pair in menu_board_items_by_x_coord.items():
                position_fk = self.get_custom_entity_pk(positions[-1])
                product_fk = pair[1]['product_fk'].value
                width = pair[1]['calculated_width_ft'].value
                # width = round(width)
                # width = 1  # this is because there is no real masking for menu board items

                self.common_v2.write_to_db_result(kpi_fk, numerator_id=product_fk, denominator_id=position_fk,
                                                  numerator_result=block_number, denominator_result=fixture_number,
                                                  result=width, score=width)
        return

    def calculate_product_above_headers(self, node_data):
        try:
            product_above_header = node_data['product_above_header'].value
        except KeyError:
            return

        if not product_above_header:
            return

        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Product Above Header')
        fixture_number = node_data['fixture_number'].value
        block_number = node_data['block_number'].value

        self.common_v2.write_to_db_result(kpi_fk, numerator_id=49, denominator_id=self.store_id,
                                          numerator_result=block_number, denominator_result=fixture_number,
                                          result=1, score=1, context_id=fixture_number)
        return

    def calculate_no_headers(self, node_data):
        try:
            no_header = node_data['no_header'].value
        except KeyError:
            return

        if not no_header:
            return

        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('No Header')
        fixture_number = node_data['fixture_number'].value
        block_number = node_data['block_number'].value

        self.common_v2.write_to_db_result(kpi_fk, numerator_id=49, denominator_id=self.store_id,
                                          numerator_result=block_number, denominator_result=fixture_number,
                                          result=1, score=1, context_id=fixture_number)
        return

    def _get_active_kpis(self):
        active_kpis = self.kpi_new_static_data[(self.kpi_new_static_data['kpi_calculation_stage_fk'] == 3) &
                                           (self.kpi_new_static_data['valid_from'] <= self.visit_date) &
                                           ((self.kpi_new_static_data['valid_until']).isnull() |
                                            (self.kpi_new_static_data['valid_until'] >= self.visit_date))]
        return active_kpis

    def calculate_facings_by_scene_type(self):
        kpi_name = 'FACINGS_BY_SCENE_TYPE'
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(kpi_name)
        if kpi_name not in self.active_kpis['type'].unique().tolist():
            return

        config = self.get_external_target_data_by_kpi_fk(kpi_fk)

        product_types = config.product_type
        template_names = config.template_name

        relevant_mpis = self.mpis[(self.mpis['product_type'].isin(product_types)) &
                                  (self.mpis['template_name'].isin(template_names))]

        relevant_mpis = relevant_mpis.groupby(['product_fk',
                                               'template_fk',
                                               'match_product_in_probe_state_fk'],
                                              as_index=False)['scene_match_fk'].count()
        relevant_mpis.rename(columns={'scene_match_fk': 'facings'}, inplace=True)

        for row in relevant_mpis.itertuples():
            self.common_v2.write_to_db_result(kpi_fk, numerator_id=row.product_fk, denominator_id=row.template_fk,
                                              context_id=row.match_product_in_probe_state_fk, result=row.facings)

        return

    def calculate_register_type(self):
        relevant_scif = self.scif[(self.scif['product_type'].isin(['POS', 'Other'])) &
                                  (self.scif['category'] == 'POS Machinery')]
        if relevant_scif.empty:
            result = 0
            product_fk = 0
        else:
            result = 1
            product_fk = relevant_scif['product_fk'].iloc[0]

        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Register Type')
        self.common_v2.write_to_db_result(kpi_fk, numerator_id=product_fk, denominator_id=self.store_id,
                                          result=result)

    def calculate_age_verification(self):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Age Verification')
        relevant_scif = self.scif[self.scif['brand_name'].isin(['Age Verification'])]

        if relevant_scif.empty:
            result = 0
            product_fk = 0

            self.common_v2.write_to_db_result(kpi_fk, numerator_id=product_fk, denominator_id=self.store_id,
                                              result=result)
        else:
            result = 1
            for product_fk in relevant_scif['product_fk'].unique().tolist():
                self.common_v2.write_to_db_result(kpi_fk, numerator_id=product_fk, denominator_id=self.store_id,
                                                  result=result)
        return

    def mark_tags_in_explorer(self, probe_match_fk_list, mpipsr_name):
        if not probe_match_fk_list:
            return
        try:
            match_type_fk = \
                self.match_product_in_probe_state_reporting[
                    self.match_product_in_probe_state_reporting['name'] == mpipsr_name][
                    'match_product_in_probe_state_reporting_fk'].values[0]
        except IndexError:
            Log.warning('Name not found in match_product_in_probe_state_reporting table: {}'.format(mpipsr_name))
            return

        match_product_in_probe_state_values_old = self.common_v2.match_product_in_probe_state_values
        match_product_in_probe_state_values_new = pd.DataFrame(columns=[MATCH_PRODUCT_IN_PROBE_FK,
                                                                        MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK])
        match_product_in_probe_state_values_new[MATCH_PRODUCT_IN_PROBE_FK] = probe_match_fk_list
        match_product_in_probe_state_values_new[MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK] = match_type_fk

        self.common_v2.match_product_in_probe_state_values = pd.concat([match_product_in_probe_state_values_old,
                                                                        match_product_in_probe_state_values_new])

        return

    def get_category_fk_by_name(self, category_name):
        return self.all_products[self.all_products['category'] == category_name]['category_fk'].iloc[0]

    def get_custom_entity_pk(self, name):
        return self.custom_entity_data[self.custom_entity_data['name'] == name]['pk'].iloc[0]

    def get_external_target_data_by_kpi_fk(self, kpi_fk):
        return self.external_targets[self.external_targets['kpi_fk'] == kpi_fk].iloc[0]

    def commit(self):
        self.common_v2.commit_results_data()
