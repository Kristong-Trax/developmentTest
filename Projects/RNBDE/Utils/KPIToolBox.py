import pandas as pd
from datetime import datetime, timedelta

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.Constants import Fields as Fd
import os
import sys

sys.path.append('.')
from Projects.RNBDE.Utils.Fetcher import RNBDEQueries
from Projects.RNBDE.Utils.GeneralToolBox import RNBDEGENERALToolBox

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


class RNBDEToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    RB = 'Reckitt Benckiser'

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
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.set_templates_data = {}
        self.download_time = timedelta(0)
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['distribution_type'].values[0]
        self.match_display_in_scene = self.get_match_display()
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.matches = self.match_product_in_scene.merge(self.all_products, on='product_fk', suffixes=['', '_1'])
        self.tools = RNBDEGENERALToolBox(self.data_provider, output,
                                         kpi_static_data=self.kpi_static_data,
                                         match_display_in_scene=self.match_display_in_scene)

    @property
    def general_tools(self):
        if not hasattr(self, '_general_tools'):
            self._general_tools = RNBDEGENERALToolBox(self.data_provider, self.output)
        return self._general_tools

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = RNBDEQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = RNBDEQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_store_retailer(self):
        query = RNBDEQueries.get_retailer(self.store_id)
        retailer = pd.read_sql_query(query, self.rds_conn.db)
        return retailer

    def main_calculation(self, set_name):
        """
        This function calculates the KPI results.
        """
        # if set_name not in self.set_templates_data.keys():
        #     calc_start_time = datetime.utcnow()
        #     self.set_templates_data[set_name] = self.tools.download_template(set_name)
        #     self.download_time += datetime.utcnow() - calc_start_time
        temp_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        retailers_brands_temp_file_path = os.path.join(temp_path, 'Retailers Brands.xlsx')
        json_data = self.tools.get_json_data(retailers_brands_temp_file_path)
        self.set_templates_data['Retailers Brands'] = json_data
        kpi_structure_path = os.path.join(temp_path, 'KPI Structure.xlsx')
        kpi_structure_json = self.tools.get_json_data(kpi_structure_path)
        kpi_structure_df = pd.DataFrame(kpi_structure_json)
        relevant_df = kpi_structure_df.loc[kpi_structure_df['KPI Level 1 Name'] == set_name]

        calc_start_time = datetime.utcnow()
        # self.set_templates_data['Footcare'] = self.tools.download_template('Footcare')

        if set_name in ('Secondary placements - Displays', 'Secondary placements - Second Placement',
                        'Secondary placements - Gondola Ends', 'Secondary placements - Location'):
            calc_start_time = datetime.utcnow()
            self.download_time += datetime.utcnow() - calc_start_time
            temp_file_path = os.path.join(temp_path, 'Second Display.xlsx')
            json_data = self.tools.get_json_data(temp_file_path)
            self.set_templates_data['Second Display'] = json_data
            category_name = 'Second Display'
        else:
            self.download_time += datetime.utcnow() - calc_start_time
            temp_file_path = os.path.join(temp_path, str(set_name) + '.xlsx')
            json_data = self.tools.get_json_data(temp_file_path)
            self.set_templates_data[set_name] = json_data
            category_name = set_name
        set_score = 0
        if category_name == 'Shelf Standards - Footcare':
            scores = []
            for kpi in relevant_df['KPI Level 2 Name'].unique().tolist():
                if 'Eye Level' in kpi:
                    kpi_score = self.calculate_footcare_eye_level_sets(set_name, category_name)
                elif 'Refill Adjacency' in kpi:
                    kpi_score = self.calculate_footcare_refill_adjacency_sets(set_name, category_name)
                elif 'Insole placement' in kpi:
                    kpi_score = self.calculate_footcare_insole_sets(set_name, category_name)
                elif 'Tights Adjacency' in kpi:
                    kpi_score = self.calculate_footcatre_tights_sets(set_name, category_name)
                else:
                    continue
                self.save_level2_and_level3(set_name, kpi, kpi_score, level_2_only=True)
                scores.append(kpi_score)
            if scores:
                set_score = sum(scores) / len(scores)
            else:
                set_score = 0
        elif category_name == 'Shelf Standards - Aircare':
            scores = []
            for kpi in relevant_df['KPI Level 2 Name'].unique().tolist():
                if 'Eye Level' in kpi:
                    kpi_score = self.calculate_aircare_eye_level_sets(set_name, category_name)
                elif 'Refill Adjacency' in kpi:
                    kpi_score = self.calculate_aircare_refill_sets(set_name, category_name)
                elif 'Spray placement' in kpi:
                    kpi_score = self.calculate_aircare_spray_sets(set_name, category_name)
                elif 'Candle/Waxmelts placement' in kpi:
                    kpi_score = self.calculate_candles_and_waxmelts_sets(set_name, category_name)
                else:
                    continue
                self.save_level2_and_level3(set_name, kpi, kpi_score, level_2_only=True)
                scores.append(kpi_score)
            if scores:
                set_score = sum(scores) / len(scores)
            else:
                set_score = 0
        elif category_name == 'Shelf Standards - ADW':
            scores = []
            for kpi in relevant_df['KPI Level 2 Name'].unique().tolist():
                if 'Eye Level' in kpi:
                    kpi_score = self.calculate_adw_eye_level_sets(set_name, category_name)
                elif 'Brand Block' in kpi:
                    kpi_score = self.calculate_adw_brand_block_sets(set_name, category_name)
                else:
                    continue
                self.save_level2_and_level3(set_name, kpi, kpi_score, level_2_only=True)
                scores.append(kpi_score)
            if scores:
                set_score = sum(scores) / len(scores)
            else:
                set_score = 0
        elif category_name == 'Shelf Standards - SWB':
            scores = []
            for kpi in relevant_df['KPI Level 2 Name'].unique().tolist():
                if 'Eye Level' in kpi:
                    kpi_score = self.calculate_swb_eye_level_sets(set_name, category_name)
                elif 'Brand Block' in kpi:
                    kpi_score = self.calculate_swb_brand_block_sets(set_name, category_name)
                elif 'Toys Adjacency' in kpi:
                    kpi_score = self.calculate_swb_toys_adjacency_sets(set_name, category_name)
                else:
                    continue
                self.save_level2_and_level3(set_name, kpi, kpi_score, level_2_only=True)
                scores.append(kpi_score)
            if scores:
                set_score = sum(scores) / len(scores)
            else:
                set_score = 0
        elif category_name == 'Shelf Standards - MPC':
            scores = []
            for kpi in relevant_df['KPI Level 2 Name'].unique().tolist():
                if 'Eye Level' in kpi:
                    kpi_score = self.calculate_mpc_eye_level_sets(set_name, category_name)
                elif 'Wipes Adjacency' in kpi:
                    kpi_score = self.calculate_wipes_adjacency_sets(set_name, category_name)
                elif 'ITB Adjacency' in kpi:
                    kpi_score = self.calculate_itb_adjacency_sets(set_name, category_name)
                elif 'MPC Segment Adjacency' in kpi:
                    kpi_score = self.calculate_mpc_segment_adjacency_sets(set_name, category_name)
                else:
                    continue
                self.save_level2_and_level3(set_name, kpi, kpi_score, level_2_only=True)
                scores.append(kpi_score)
            if scores:
                set_score = sum(scores) / len(scores)
            else:
                set_score = 0
        elif category_name == 'Second Display':
            if set_name == 'Secondary placements - Displays':
                set_score = self.calculate_displays_sets(set_name, category_name)
            elif set_name == 'Secondary placements - Gondola Ends':
                set_score = self.calculate_gondola_ends_sets(set_name, category_name)
            elif set_name == 'Secondary placements - Second Placement':
                set_score = self.calculate_second_placement_sets(set_name, category_name)
            elif set_name == 'Secondary placements - Location':
                set_score = self.calculate_location_sets(set_name, category_name)
        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
        if set_score is not None:
            self.write_to_db_result(set_fk, round(set_score, 2), self.LEVEL1)
        else:
            self.write_to_db_result(set_fk, set_score, self.LEVEL1)
        return set_score

    def calculate_shelf_position(self, params):
        shelf_target = [int(target) for target in params.get(self.tools.SHELF_TARGET).split(',')]
        try:
            shelf_position_result = self.shelf_level_assortment(params.get(self.tools.TESTED).split(','),
                                                                shelf_target)
        except AttributeError as e:
            shelf_position_result = self.shelf_level_assortment([params.get(self.tools.TESTED)],
                                                                shelf_target)
        return shelf_position_result

    def calculate_adjacency(self, params, custom_direction_data=None):
        if not custom_direction_data:
            direction_data = {'right': 1, 'left': 1, 'top': 1, 'bottom': 1}
        else:
            direction_data = custom_direction_data
        tested_filters = {'product_ean_code': params.get(self.tools.TESTED).split(',')}
        anchor_filters = {'product_ean_code': params.get(self.tools.ANCHOR).split(',')}
        initial_result = self.tools.calculate_relative_position(tested_filters=tested_filters,
                                                                anchor_filters=anchor_filters,
                                                                direction_data=direction_data)
        return initial_result

    def calculate_negative_block(self, params):
        block_result_1 = self.tools.calculate_block_together(
            product_ean_code=params.get(self.tools.TESTED).split(','))
        block_result_2 = self.tools.calculate_block_together(
            product_ean_code=params.get(self.tools.ANCHOR).split(','))
        if block_result_1 and block_result_2:
            tested_filters = {'product_ean_code': params.get(self.tools.TESTED).split(',')}
            anchor_filters = {'product_ean_code': params.get(self.tools.ANCHOR).split(',')}
            total_score = self.tools.calculate_non_proximity(tested_filters=tested_filters,
                                                             anchor_filters=anchor_filters,
                                                             allowed_diagonal=True)
        else:
            total_score = 0
        return total_score

    def calculate_block_kpi(self, params):
        all_products_list = []
        block_result_1 = self.tools.calculate_block_together(
            product_ean_code=params.get(self.tools.TESTED).split(','))
        all_products_list.extend(params.get(self.tools.TESTED).split(','))
        if params.get(self.tools.ANCHOR) is not None:
            block_result_2 = self.tools.calculate_block_together(
                product_ean_code=params.get(self.tools.ANCHOR).split(','))
            all_products_list.extend(params.get(self.tools.ANCHOR).split(','))
            if block_result_1 and block_result_2:
                total_score = self.tools.calculate_block_together(
                    product_ean_code=all_products_list)
            else:
                total_score = 0
        else:
            if block_result_1:
                total_score = 1
            else:
                total_score = 0
        return total_score

    def calculate_negative_adjacenecy(self, params):
        direction_data = {'right': 1, 'left': 1, 'top': 1, 'bottom': 1}
        tested_filters = {'product_ean_code': params.get(self.tools.TESTED).split(',')}
        anchor_filters = {'product_ean_code': params.get(self.tools.ANCHOR).split(',')}
        initial_result = self.tools.calculate_relative_position(tested_filters=tested_filters,
                                                                anchor_filters=anchor_filters,
                                                                direction_data=direction_data)
        if initial_result:
            total_score = 0
        else:
            total_score = 1
        return total_score

    def calculate_footcare_eye_level_sets(self, set_name, category_name):
        kpi_results_dict = {'Gadgets': 1000, 'Pink Adj. to Blue': 1000, 'Hardskin & Nail Separation': 1000,
                            'Hardskin & Nail Block': 1000}
        for params in self.set_templates_data[category_name]:
            if not 'Eye Level' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Shelf Position':
                shelf_position_result = self.calculate_shelf_position(params)
                kpi_results_dict['Gadgets'] = min(shelf_position_result, kpi_results_dict['Gadgets'])
                # kpi_results_dict['Gadgets'] += shelf_position_result
            elif params.get(self.tools.FORMULA) == 'Adjacency':
                initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict['Pink Adj. to Blue'] = min(result, kpi_results_dict['Pink Adj. to Blue'])
            elif params.get(self.tools.FORMULA) == 'Block (N)':
                score = self.calculate_negative_block(params)
                # kpi_results_dict['Hardskin & Nail Separation'] = min(score, kpi_results_dict['Hardskin & Nail Separation'])
                result = 1 if score else 0
                kpi_results_dict['Hardskin & Nail Separation'] = min(result,
                                                                     kpi_results_dict['Hardskin & Nail Separation'])
            elif params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict['Hardskin & Nail Block'] = min(score, kpi_results_dict['Hardskin & Nail Block'])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Eye Level')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_footcare_refill_adjacency_sets(self, set_name, category_name):
        kpi_results_dict = {'Refill Adj. to Gadgets': 1000, 'Refill Separation': 1000}

        for params in self.set_templates_data[category_name]:
            if not 'Refill Adjacency' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Adjacency':
                initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict['Refill Adj. to Gadgets'] = min(result, kpi_results_dict['Refill Adj. to Gadgets'])
            elif params.get(self.tools.FORMULA) == 'Adjacency (N)':
                score = self.calculate_negative_adjacenecy(params)
                kpi_results_dict['Refill Separation'] = min(score, kpi_results_dict['Refill Separation'])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Refill Adjacency')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_footcare_insole_sets(self, set_name, category_name):
        kpi_results_dict = {'Insoles on top-shelf': 1000, 'Large Block': 1000, 'Women Block': 1000,
                            'Women Adj. to Large': 1000}

        for params in self.set_templates_data[category_name]:
            if not 'Insole placement' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Shelf Position':
                shelf_position_result = self.calculate_shelf_position(params)
                kpi_results_dict[params.get('KPI')] = min(shelf_position_result, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Adjacency':
                initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict[params.get('KPI')] = min(result, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict[params.get('KPI')] = min(score, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Insole placement')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_footcatre_tights_sets(self, set_name, category_name):
        kpi_results_dict = {'Tights Adj. to Insoles': 1000, 'Tights Block': 1000}

        for params in self.set_templates_data[category_name]:
            if not 'Tights Adjacency' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Adjacency (N)':
                score = self.calculate_negative_adjacenecy(params)
                kpi_results_dict['Tights Adj. to Insoles'] = min(score, kpi_results_dict['Tights Adj. to Insoles'])
            elif params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict['Tights Block'] = min(score, kpi_results_dict['Tights Block'])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Tights Adjacency')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_aircare_eye_level_sets(self, set_name, category_name):
        kpi_results_dict = {'FreshMatic Gadgets': 1000, 'Liquid Electrical Gadgets': 1000}
        for params in self.set_templates_data[category_name]:
            if not 'Eye Level' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Shelf Position':
                shelf_position_result = self.calculate_shelf_position(params)
                kpi_results_dict[params.get('KPI')] = min(shelf_position_result, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Eye Level')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_aircare_refill_sets(self, set_name, category_name):
        kpi_results_dict = {'FreshMatic Refill Adj. to Gadgets': 1000, 'Liquid Electrical Refill Adj. to Gadgets': 1000,
                            'Liquid Electrical refill Blocking': 1000, 'FreshMatic MAX refill Blocking': 1000}

        for params in self.set_templates_data[category_name]:
            if not 'Refill Adjacency' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Adjacency':
                initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict[params.get('KPI')] = min(result, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict[params.get('KPI')] = min(score, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Refill Adjacency')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_aircare_spray_sets(self, set_name, category_name):
        kpi_results_dict = {'Spray Block': 1000, 'Spray Adjacency': 1000}

        for params in self.set_templates_data[category_name]:
            if not 'Spray placement' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Adjacency':
                initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict[params.get('KPI')] = min(result, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict[params.get('KPI')] = min(score, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Spray placement')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_candles_and_waxmelts_sets(self, set_name, category_name):
        kpi_results_dict = {'Candles on shelf 2/3': 1000, 'Waxmelts Adj. to Candles': 1000}
        for params in self.set_templates_data[category_name]:
            if not 'Candle/Waxmelts placement' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Shelf Position':
                shelf_position_result = self.calculate_shelf_position(params)
                kpi_results_dict[params.get('KPI')] = min(shelf_position_result, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Adjacency':
                initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict[params.get('KPI')] = min(result, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict[params.get('KPI')] = min(score, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Candle/Waxmelts placement')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_adw_eye_level_sets(self, set_name, category_name):
        kpi_results_dict = {'Finish Quantum': 1000, 'Finish Additive': 1000}
        for params in self.set_templates_data[category_name]:
            if not 'Eye Level' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Shelf Position':
                shelf_position_result = self.calculate_shelf_position(params)
                kpi_results_dict[params.get('KPI')] = min(shelf_position_result, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict[params.get('KPI')] = min(score, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Eye Level')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_adw_brand_block_sets(self, set_name, category_name):
        kpi_results_dict = {'Finish Detergents': 1000, 'Additives Adj. to Detergents': 1000,
                            'Salt Adj. to Detergents': 1000}
        for params in self.set_templates_data[category_name]:
            if not 'Brand Block' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Adjacency':
                initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict[params.get('KPI')] = min(result, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict[params.get('KPI')] = min(score, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Brand Block')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_swb_eye_level_sets(self, set_name, category_name):
        kpi_results_dict = {'Premium Condoms': 1000, 'Lubricants': 1000, 'Toys': 1000}
        for params in self.set_templates_data[category_name]:
            if not 'Eye Level' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Shelf Position':
                shelf_position_result = self.calculate_shelf_position(params)
                kpi_results_dict[params.get('KPI')] = min(shelf_position_result,
                                                          kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Adjacency':
                initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict[params.get('KPI')] = min(result, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict[params.get('KPI')] = min(score, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Eye Level')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_swb_toys_adjacency_sets(self, set_name, category_name):
        kpi_results_dict = {'Toys Adj. to Lubricants': 1000}
        for params in self.set_templates_data[category_name]:
            if not 'Toys Adjacency' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Adjacency':
                initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict[params.get('KPI')] = min(result, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Toys Adjacency')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_swb_brand_block_sets(self, set_name, category_name):
        kpi_results_dict = {'Condoms': 1000, 'Lubricants Adj. to Condoms': 1000}
        for params in self.set_templates_data[category_name]:
            if not 'Brand Block' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict[params.get('KPI')] = min(score, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Adjacency':
                if params.get('KPI') == 'Lubricants Adj. to Condoms':
                    direction_data = {'right': 1, 'left': 1, 'top': 1, 'bottom': 0}
                    initial_result = self.calculate_adjacency(params, custom_direction_data=direction_data)
                else:
                    initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict[params.get('KPI')] = min(result, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Brand Block')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_wipes_adjacency_sets(self, set_name, category_name):
        kpi_results_dict = {'Sagrotan Block': 1000, 'Sagrotan Adj. to PL': 1000}
        for params in self.set_templates_data[category_name]:
            if not 'Wipes Adjacency' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Adjacency':
                initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict[params.get('KPI')] = min(result, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict[params.get('KPI')] = min(score, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Wipes Adjacency')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_itb_adjacency_sets(self, set_name, category_name):
        kpi_results_dict = {'Cillit Block': 1000, 'Cillit Adj. to WC': 1000}
        for params in self.set_templates_data[category_name]:
            if not 'ITB Adjacency' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Adjacency':
                initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict[params.get('KPI')] = min(result, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict[params.get('KPI')] = min(score, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='ITB Adjacency')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_mpc_segment_adjacency_sets(self, set_name, category_name):
        kpi_results_dict = {'Bath-Cleaner': 1000, 'Kitchen-Cleaner': 1000, 'Dilutables': 1000}
        for params in self.set_templates_data[category_name]:
            if not 'MPC' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Adjacency':
                initial_result = self.calculate_adjacency(params)
                result = 1 if initial_result else 0
                kpi_results_dict[params.get('KPI')] = min(result, kpi_results_dict[params.get('KPI')])
            elif params.get(self.tools.FORMULA) == 'Block':
                score = self.calculate_block_kpi(params)
                kpi_results_dict[params.get('KPI')] = min(score, kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='MPC Segment Adjacency')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_mpc_eye_level_sets(self, set_name, category_name):
        kpi_results_dict = {'Sagrotan Desincetion': 1000}
        for params in self.set_templates_data[category_name]:
            if not 'SWB' in params.get('KPI Level 2 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'Shelf Position':
                shelf_position_result = self.calculate_shelf_position(params)
                kpi_results_dict[params.get('KPI')] = min(shelf_position_result,
                                                          kpi_results_dict[params.get('KPI')])
            else:
                continue
        for kpi in kpi_results_dict.keys():
            if kpi_results_dict[kpi] == 1000:
                kpi_results_dict[kpi] = 0
            self.save_level2_and_level3(set_name, kpi, kpi_results_dict[kpi],
                                        level_3_only=True,
                                        level2_name_for_atomic='Eye Level')
        if kpi_results_dict:
            kpi_score = (sum(kpi_results_dict.values()) / float(len(kpi_results_dict))) * 100
        else:
            kpi_score = 0

        return round(kpi_score, 2)

    def calculate_displays_sets(self, set_name, category_name):
        set_results_dict = {}
        params_df = pd.DataFrame(self.set_templates_data[category_name])
        relevant_df = params_df.loc[params_df['KPI Level 1 Name'].str.contains('Displays')]
        for level2 in relevant_df['KPI Level 2 Name'].unique().tolist():
            set_results_dict[level2] = 0
        for params in self.set_templates_data[category_name]:
            if 'Displays' not in params.get('KPI Level 1 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'count_display':
                try:
                    display_fks = [int(display) for display in params.get('Parameter').split(',')]
                except AttributeError as e:
                    display_fks = [params.get('Parameter')]
                score = len(self.match_display_in_scene.loc[
                                self.match_display_in_scene['display_fk'].isin(display_fks)]['display_fk'].tolist())
                self.save_level2_and_level3(set_name, params.get('KPI Level 3 Name'), score, level_3_only=True,
                                            level2_name_for_atomic=params.get('KPI Level 2 Name'))
                set_results_dict[params.get('KPI Level 2 Name')] += score
            elif params.get(self.tools.FORMULA) == 'count_facings':
                try:
                    display_fks = [int(display) for display in params.get('in Scenes with ScR tags (where)').split(',')]
                except AttributeError as e:
                    display_fks = [params.get('in Scenes with ScR tags (where)')]
                if display_fks:
                    scenes_filter = self.match_display_in_scene.loc[
                        self.match_display_in_scene['display_fk'].isin(display_fks)]['scene_fk'].unique().tolist()
                else:
                    scenes_filter = self.match_display_in_scene['scene_fk'].unique().tolist()
                manufacturer_param = params.get('Parameter')
                if manufacturer_param in ('Air Wick', 'Ceraclen', 'Cillit', 'Durex', 'Finish', 'Finish Prof.',
                                            'Sagrotan', 'Scholl'):
                    score = self.scif.loc[(self.scif[Fd.B_NAME] == manufacturer_param) & (
                        self.scif['scene_fk'].isin(scenes_filter))]['facings'].sum()
                else:
                    score = self.scif.loc[(self.scif['manufacturer_name'] == manufacturer_param) & (
                        self.scif['scene_fk'].isin(scenes_filter))]['facings'].sum()
                self.save_level2_and_level3(set_name, params.get('KPI Level 3 Name'), score, level_3_only=True,
                                            level2_name_for_atomic=params.get('KPI Level 2 Name'))
                set_results_dict[params.get('KPI Level 2 Name')] += score
            elif params.get(self.tools.FORMULA) == 'linear_sos':  # todo include empty and other
                # nominator = (self.scif.loc[self.scif[Fd.M_NAME] == self.RB])
                # denominator = (self.scif)
                # score = self.tools.calculate_sos_by_linear(nominator, denominator)
                try:
                    display_fk_filter = [int(display) for display in
                                         params.get('in Scenes with ScR tags (where)').split(',')]
                except AttributeError as e:
                    display_fk_filter = [params.get('in Scenes with ScR tags (where)')]
                scenes_filter_displays = self.match_display_in_scene.loc[
                    self.match_display_in_scene['display_fk'].isin(display_fk_filter)]['scene_fk'].unique().tolist()
                nominator = self.matches.loc[(self.matches[Fd.M_NAME] == self.RB)
                                             & (self.matches['stacking_layer'] == 1) & (
                                             self.matches['scene_fk'].isin(scenes_filter_displays))]['width_mm'].sum()
                denominator = self.matches.loc[(self.matches['stacking_layer'] == 1)& (
                                             self.matches['scene_fk'].isin(scenes_filter_displays))]['width_mm'].sum()
                if denominator:
                    score = (nominator / float(denominator)) * 100
                else:
                    score = 0
                self.save_level2_and_level3(set_name, params.get('KPI Level 3 Name'), score, level_3_only=True,
                                            level2_name_for_atomic=params.get('KPI Level 2 Name'))
                set_results_dict[params.get('KPI Level 2 Name')] += score
        for kpi in set_results_dict.keys():
            self.save_level2_and_level3(set_name, kpi, set_results_dict[kpi], level_2_only=True)
        set_score = None
        return set_score

    def calculate_gondola_ends_sets(self, set_name, category_name):
        set_results_dict = {}
        params_df = pd.DataFrame(self.set_templates_data[category_name])
        relevant_df = params_df.loc[params_df['KPI Level 1 Name'].str.contains('Gondola Ends')]
        for level2 in relevant_df['KPI Level 2 Name'].unique().tolist():
            set_results_dict[level2] = 0
        for params in self.set_templates_data[category_name]:
            if 'Gondola Ends' not in params.get('KPI Level 1 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'count_display':
                try:
                    display_fk_filter = [int(display) for display in
                                         params.get('in Scenes with ScR tags (where)').split(',')]
                except AttributeError as e:
                    display_fk_filter = [params.get('in Scenes with ScR tags (where)')]
                scenes_filter_displays = self.match_display_in_scene.loc[
                    self.match_display_in_scene['display_fk'].isin(display_fk_filter)]['scene_fk'].unique().tolist()
                try:
                    display_fks = [int(display) for display in params.get('Parameter').split(',')]
                except AttributeError as e:
                    display_fks = [params.get('Parameter')]
                score = len(self.match_display_in_scene.loc[
                                (self.match_display_in_scene['display_fk'].isin(display_fks)) & (
                                    self.match_display_in_scene['scene_fk'].isin(scenes_filter_displays))][
                                'display_fk'].tolist())
                self.save_level2_and_level3(set_name, params.get('KPI Level 3 Name'), score, level_3_only=True,
                                            level2_name_for_atomic=params.get('KPI Level 2 Name'))
                set_results_dict[params.get('KPI Level 2 Name')] += score
            elif params.get(self.tools.FORMULA) == 'custom_rb_facings':
                if params.get('specified store type') != self.store_type:
                    continue
                try:
                    display_fks = [int(display) for display in params.get('in Scenes with ScR tags (where)').split(',')]
                except AttributeError as e:
                    display_fks = [params.get('in Scenes with ScR tags (where)')]
                scenes_filter = self.match_display_in_scene.loc[
                    self.match_display_in_scene['display_fk'].isin(display_fks)]['scene_fk'].unique().tolist()
                manufacturer_param = params.get('Parameter')
                if manufacturer_param == 'Private Label':
                    store_retailer = self.get_store_retailer()
                    retailer_fk = store_retailer['retailer_fk'].values[0]
                    retailers_brands = pd.DataFrame(self.set_templates_data['Retailers Brands'])
                    relevant_param = \
                        retailers_brands.loc[retailers_brands['retailer_fk'] == retailer_fk]['brand_fk'].values[0]
                    score = self.scif.loc[(self.scif[Fd.B_FK] == relevant_param) & (
                        self.scif['scene_fk'].isin(scenes_filter))]['facings'].sum()
                elif manufacturer_param in ('Air Wick', 'Ceraclen', 'Cillit', 'Durex', 'Finish', 'Finish Prof.',
                                            'Sagrotan', 'Scholl'):
                    score = self.scif.loc[(self.scif[Fd.B_NAME] == manufacturer_param) & (
                        self.scif['scene_fk'].isin(scenes_filter))]['facings'].sum()
                else:
                    score = self.scif.loc[(self.scif[Fd.M_NAME] == manufacturer_param) & (
                        self.scif['scene_fk'].isin(scenes_filter))]['facings'].sum()
                self.save_level2_and_level3(set_name, params.get('KPI Level 3 Name'), score, level_3_only=True,
                                            level2_name_for_atomic=params.get('KPI Level 2 Name'))
                set_results_dict[params.get('KPI Level 2 Name')] += score
            elif params.get(self.tools.FORMULA) == 'bonus_shelves':  # todo include empty and other
                try:
                    display_fks = [int(display) for display in params.get('in Scenes with ScR tags (where)').split(',')]
                except AttributeError as e:
                    display_fks = [params.get('in Scenes with ScR tags (where)')]
                scenes_filter = self.match_display_in_scene.loc[
                    self.match_display_in_scene['display_fk'].isin(display_fks)]['scene_fk'].unique().tolist()
                scenes_filter_displays = \
                    self.match_display_in_scene.loc[self.match_display_in_scene['scene_fk'].isin(scenes_filter)][
                        'display_fk'].unique().tolist()
                if len(scenes_filter_displays) > 1:
                    try:
                        shelves_target = params.get('Target').split(',')
                    except AttributeError as e:
                        shelves_target = [params.get('Target')]
                    shelves_result = []
                    for scene in scenes_filter:
                        for shelf in shelves_target:
                            shelf_filter = self.matches.loc[(self.matches['scene_fk'] == scene)
                                                            & (self.matches['shelf_number'] == shelf)]
                            if len(shelf_filter[Fd.M_NAME].unique().tolist()) == 1 and self.RB == \
                                    shelf_filter[Fd.M_NAME].values[0]:
                                result = 1
                            else:
                                result = 0
                            shelves_result.append(result)
                else:
                    shelves_result = []
                    for scene in scenes_filter:
                        for shelf in self.matches.loc[(self.matches['scene_fk'] == scene)]['shelf_number'].unique().tolist():
                            shelf_filter = self.matches.loc[(self.matches['scene_fk'] == scene)
                                                            & (self.matches['shelf_number'] == shelf)]
                            if len(shelf_filter[Fd.M_NAME].unique().tolist()) == 1 and self.RB == \
                                    shelf_filter[Fd.M_NAME].values[0]:
                                result = 1
                            else:
                                result = 0
                            shelves_result.append(result)
                self.save_level2_and_level3(set_name, params.get('KPI Level 3 Name'), sum(shelves_result),
                                            level_3_only=True,
                                            level2_name_for_atomic=params.get('KPI Level 2 Name'))
                set_results_dict[params.get('KPI Level 2 Name')] += sum(shelves_result)
            elif params.get(self.tools.FORMULA) == 'shelves_linear_sos':  # todo include empty and other
                try:
                    display_fks = [int(display) for display in params.get('in Scenes with ScR tags (where)').split(',')]
                except AttributeError as e:
                    display_fks = [params.get('in Scenes with ScR tags (where)')]
                scenes_filter = self.match_display_in_scene.loc[
                    self.match_display_in_scene['display_fk'].isin(display_fks)]['scene_fk'].unique().tolist()
                scenes_filter_displays = \
                    self.match_display_in_scene.loc[self.match_display_in_scene['scene_fk'].isin(scenes_filter)][
                        'display_fk'].unique().tolist()
                if len(scenes_filter_displays) > 1:
                    try:
                        shelves_target = params.get('Target').split(',')
                    except AttributeError as e:
                        shelves_target = [params.get('Target')]
                    nominator = int(self.matches.loc[
                                        (self.matches['scene_fk'].isin(scenes_filter)) & (
                                            self.matches[Fd.M_NAME] == self.RB) & (
                                            self.matches['shelf_number'].isin(shelves_target)) & (
                                            self.matches['stacking_layer'] == 1)]['width_mm'].values.sum())
                    denominator = int(self.matches.loc[(self.matches['scene_fk'].isin(scenes_filter)) & (
                        self.matches['shelf_number'].isin(shelves_target)) & (self.matches['stacking_layer'] == 1)][
                                          'width_mm'].values.sum())
                else:
                    nominator = int(self.matches.loc[
                                        (self.matches['scene_fk'].isin(scenes_filter)) & (
                                            self.matches[Fd.M_NAME] == self.RB)][
                                        'width_mm'].values.sum())
                    denominator = int(
                        self.matches.loc[(self.matches['scene_fk'].isin(scenes_filter))]['width_mm'].values.sum())
                if denominator:
                    score = (nominator / float(denominator)) * 100
                else:
                    score = 0
                self.save_level2_and_level3(set_name, params.get('KPI Level 3 Name'), score, level_3_only=True,
                                            level2_name_for_atomic=params.get('KPI Level 2 Name'))
                set_results_dict[params.get('KPI Level 2 Name')] += score
            else:
                pass
        for kpi in set_results_dict.keys():
            self.save_level2_and_level3(set_name, kpi, set_results_dict[kpi], level_2_only=True)
        set_score = None
        return set_score

    def calculate_second_placement_sets(self, set_name, category_name):
        set_results_dict = {}
        params_df = pd.DataFrame(self.set_templates_data[category_name])
        relevant_df = params_df.loc[params_df['KPI Level 1 Name'].str.contains('Second Placement')]
        for level2 in relevant_df['KPI Level 2 Name'].unique().tolist():
            set_results_dict[level2] = 0
        for params in self.set_templates_data[category_name]:
            if 'Second Placement' not in params.get('KPI Level 1 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'count_display':
                try:
                    display_fks = [int(display) for display in params.get('Parameter').split(',')]
                except AttributeError as e:
                    display_fks = [params.get('Parameter')]
                score = len(self.match_display_in_scene.loc[
                                self.match_display_in_scene['display_fk'].isin(display_fks)]['display_fk'].tolist())
                self.save_level2_and_level3(set_name, params.get('KPI Level 3 Name'), score, level_3_only=True,
                                            level2_name_for_atomic=params.get('KPI Level 2 Name'))
                set_results_dict[params.get('KPI Level 2 Name')] += score
        for kpi in set_results_dict.keys():
            self.save_level2_and_level3(set_name, kpi, set_results_dict[kpi], level_2_only=True)
        set_score = sum(set_results_dict.values())
        return set_score

    def calculate_location_sets(self, set_name, category_name):
        set_results_dict = {}
        params_df = pd.DataFrame(self.set_templates_data[category_name])
        relevant_df = params_df.loc[params_df['KPI Level 1 Name'].str.contains('Location')]
        for level2 in relevant_df['KPI Level 2 Name'].unique().tolist():
            set_results_dict[level2] = 0
        for params in self.set_templates_data[category_name]:
            if 'Location' not in params.get('KPI Level 1 Name'):
                continue
            if params.get(self.tools.FORMULA) == 'count_display':
                try:
                    display_fks = [int(display) for display in params.get('Parameter').split(',')]
                except AttributeError as e:
                    display_fks = [params.get('Parameter')]
                try:
                    templates = [template for template in params.get('in Scenes with ScR tags (where)').split(',')]
                except AttributeError as e:
                    templates = [params.get('in Scenes with ScR tags (where)')]
                scenes_filter = self.scif.loc[self.scif[Fd.T_NAME].isin(templates)]['scene_fk'].unique().tolist()
                score = len(self.match_display_in_scene.loc[
                                (self.match_display_in_scene['display_fk'].isin(display_fks)) &
                                (self.match_display_in_scene['scene_fk'].isin(scenes_filter))]['display_fk'].tolist())
                self.save_level2_and_level3(set_name, params.get('KPI Level 3 Name'), score, level_3_only=True,
                                            level2_name_for_atomic=params.get('KPI Level 2 Name'))
                set_results_dict[params.get('KPI Level 2 Name')] += score
        for kpi in set_results_dict.keys():
            self.save_level2_and_level3(set_name, kpi, set_results_dict[kpi], level_2_only=True)
        set_score = sum(set_results_dict.values())
        return set_score

    def shelf_level_assortment(self, products_list, shelf_target, strict=True, **filters):
        filters, relevant_scenes = self.tools.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            relevant_scenes = self.scif['scene_fk'].unique().tolist()
        # number_of_products = len(
        #     self.all_products[self.tools.get_filter_condition(self.all_products, **filters)]['product_ean_code'])
        number_of_misplaced_products = 0
        result = 0  # Default score is FALSE
        scenes_results = []
        for scene in relevant_scenes:
            eye_level_facings = pd.DataFrame(columns=self.match_product_in_scene.columns)
            matches = pd.merge(self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene],
                               self.all_products, on=['product_fk'])
            for bay in matches['bay_number'].unique():
                bay_matches = matches[matches['bay_number'] == bay]
                products_in_target_shelf = bay_matches[(bay_matches['shelf_number'].isin(shelf_target)) & (
                    bay_matches['product_ean_code'].isin(products_list))]
                products_not_in_target_shelf = bay_matches[(~bay_matches['shelf_number'].isin(shelf_target)) & (
                    bay_matches['product_ean_code'].isin(products_list))]
                if not products_not_in_target_shelf.empty:
                    number_of_misplaced_products += 1
                eye_level_facings = eye_level_facings.append(products_in_target_shelf)
            # eye_level_assortment = len(eye_level_facings[
            #                                self.tools.get_filter_condition(eye_level_facings, **filters)][
            #                                'product_ean_code'])
            eye_level_assortment = len(eye_level_facings['product_fk'])
            # eye_level_assortment = len(eye_level_facings['product_ean_code'])
            if strict:
                if eye_level_assortment >= 1:
                    if number_of_misplaced_products > 0:
                        result = 0
                    else:
                        result = 1
                else:
                    result = 0
            else:
                if eye_level_assortment >= 1:
                    result = 1
            scenes_results.append(result)

        if scenes_results:
            final_result = max(scenes_results)
        else:
            final_result = 0

        return final_result

    def save_level2_and_level3(self, set_name, kpi_name, result, score=None, threshold=None, level_2_only=False,
                               level_3_only=False, level2_name_for_atomic=None):
        """
        Given KPI data and a score, this functions writes the score for both KPI level 2 and 3 in the DB.
        """
        try:
            if level_2_only:
                kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                                (self.kpi_static_data['kpi_name'] == kpi_name)]
                kpi_fk = kpi_data['kpi_fk'].values[0]
                self.write_to_db_result(kpi_fk, result, self.LEVEL2)
            elif level_3_only:
                kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                                (self.kpi_static_data['kpi_name'] == level2_name_for_atomic) & (
                                                    self.kpi_static_data['atomic_kpi_name'] == kpi_name)]
                # kpi_fk = kpi_data['kpi_fk'].values[0]
                atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
                self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3, score=score)
            else:
                kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                                (self.kpi_static_data['kpi_name'] == kpi_name)]
                kpi_fk = kpi_data['kpi_fk'].values[0]
                atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
                self.write_to_db_result(kpi_fk, result, self.LEVEL2)
                if score is None and threshold is None:
                    self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3)
                elif score is not None and threshold is None:
                    self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3, score=score)
                else:
                    self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3, score=score, threshold=threshold)
        except IndexError as e:
            Log.info('KPI {} is not defined in the DB'.format(kpi_name))

    def write_to_db_result(self, fk, result, level, score=None, threshold=None):
        """
        This function the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, result, level, score=score, threshold=threshold)
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

    def create_attributes_dict(self, fk, result, level, score2=None, score=None, threshold=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        # result = round(result, 2)
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            score_type = '%' if kpi_set_name in self.tools.KPI_SETS_WITH_PERCENT_AS_SCORE else ''
            # attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
            #                             format(result, '.2f'), score_type, fk)],
            #                           columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
            #                                    'score_2', 'kpi_set_fk'])
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        result, score_type, fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'score_2', 'kpi_set_fk'])

        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'",
                                                                                                                "\\'")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, result)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0].replace("'", "\\'")
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            if not score and not threshold:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                            result, kpi_fk, fk, None, result)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                                   'score'])
            else:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                            result, kpi_fk, fk, threshold, score)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk',
                                                   'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk',
                                                   'threshold',
                                                   'score'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = RNBDEQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
