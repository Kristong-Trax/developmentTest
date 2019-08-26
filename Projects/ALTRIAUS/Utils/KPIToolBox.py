import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
import os
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Projects.ALTRIAUS.Utils.ParseTemplates import parse_template
import datetime
from Projects.ALTRIAUS.Utils.AltriaDataProvider import AltriaDataProvider
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nicolaske'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
CATEGORIES = ['Cigarettes', 'Vapor', 'Cigars', 'Smokeless']
KPI_LEVEL_2_cat_space = ['Category Space - Cigarettes', 'Category Space - Vapor',
                         'Category Space - Smokeless', 'Category Space - Cigars']

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'KENGINE_ALTRIA_V1.xlsx')
FIXTURE_WIDTH_TEMPLATE = \
    os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Fixture_Width_Dimensions_v3.xlsx')

NO_FLIP_SIGN_PK = 11161


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


class ALTRIAUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

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
        self.thresholds_and_results = {}
        self.result_df = []
        self.writing_to_db_time = datetime.timedelta(0)
        self.kpi_results_queries = []
        self.potential_products = {}
        self.shelf_square_boundaries = {}
        self.average_shelf_values = {}
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.all_template_data = parse_template(TEMPLATE_PATH, "KPI")
        self.spacing_template_data = parse_template(TEMPLATE_PATH, "Spacing")
        self.fixture_width_template = pd.read_excel(FIXTURE_WIDTH_TEMPLATE, "Fixture Width", dtype=pd.Int64Dtype())
        self.facings_to_feet_template = pd.read_excel(FIXTURE_WIDTH_TEMPLATE, "Conversion Table", dtype=pd.Int64Dtype())
        self.header_positions_template = pd.read_excel(FIXTURE_WIDTH_TEMPLATE, "Header Positions")
        self.flip_sign_positions_template = pd.read_excel(FIXTURE_WIDTH_TEMPLATE, "Flip Sign Positions")
        self.custom_entity_data = self.ps_data_provider.get_custom_entities(1005)
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.INCLUDE_FILTER = 1
        self.assortment = Assortment(self.data_provider, output=self.output, ps_data_provider=self.ps_data_provider)
        self.store_assortment = self.assortment.get_lvl3_relevant_ass()

        self.kpi_new_static_data = self.common.get_new_kpi_static_data()
        try:
            self.mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
                        .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
                          .merge(self.template_info, on='template_fk', suffixes=['', '_t'])
        except KeyError:
            Log.warning('MPIS cannot be generated!')
            return
        self.adp = AltriaDataProvider(self.data_provider)

    def main_calculation(self, *args, **kwargs):
        """
               This function calculates the KPI results.
               """
        self.calculate_signage_locations_and_widths('Cigarettes')
        self.calculate_signage_locations_and_widths('Smokeless')
        self.calculate_register_type()
        self.calculate_age_verification()
        self.calculate_assortment()
        self.calculate_vapor_kpis()

        kpi_set_fk = 2
        set_name = \
            self.kpi_static_data.loc[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].values[0]
        template_data = self.all_template_data.loc[self.all_template_data['KPI Level 1 Name'] == set_name]

        try:
            if set_name and not set(template_data['Scene Types to Include'].values[0].encode().split(', ')) & set(
                    self.scif['template_name'].unique().tolist()):
                Log.info('Category {} was not captured'.format(template_data['category'].values[0]))
                return
        except Exception as e:
            Log.info('KPI Set {} is not defined in the template'.format(set_name))

        for i, row in template_data.iterrows():
            try:
                kpi_name = row['KPI Level 2 Name']
                if kpi_name in KPI_LEVEL_2_cat_space:
                    # scene_type = [s for s in row['Scene_Type'].encode().split(', ')]
                    kpi_type = row['KPI Type']
                    scene_type = row['scene_type']

                    if row['Param1'] == 'Category' or 'sub_category':
                        category = row['Value1']

                        if kpi_type == 'category_space':
                            kpi_set_fk = \
                            self.kpi_new_static_data.loc[self.kpi_new_static_data['type'] == kpi_type]['pk'].values[0]
                            self.calculate_category_space(kpi_set_fk, kpi_name, category, scene_types=scene_type)

            except Exception as e:
                Log.info('KPI {} calculation failed due to {}'.format(kpi_name.encode('utf-8'), e))
                continue
        return

    def calculate_vapor_kpis(self):
        category = 'Vapor'
        relevant_scif = self.scif[self.scif['template_name'] == 'JUUL Merchandising']
        if relevant_scif.empty:
            Log.info('No products found for {} category'.format(category))
            return

        relevant_scif = relevant_scif[(relevant_scif['category'].isin([category, 'POS'])) &
                                      (relevant_scif['brand_name'] == 'Juul')]
        if relevant_scif.empty:
            return
        relevant_product_pks = relevant_scif[relevant_scif['product_type'] == 'SKU']['product_fk'].unique().tolist()
        relevant_scene_id = self.get_most_frequent_scene(relevant_scif)
        product_mpis = self.mpis[(self.mpis['product_fk'].isin(relevant_product_pks)) &
                                 (self.mpis['scene_fk'] == relevant_scene_id)]

        if product_mpis.empty:
            Log.info('No products found for {} category'.format(category))
            return

        self.calculate_total_shelves(product_mpis, category, product_mpis)

        longest_shelf = \
            product_mpis[product_mpis['shelf_number'] ==
                         self.get_longest_shelf_number(product_mpis,
                                                       max_shelves_from_top=999)].sort_values(by='rect_x',
                                                                                              ascending=True)

        if longest_shelf.empty or longest_shelf.isnull().all().all():
            Log.warning(
                'The {} category items are in a non-standard location. The {} category will not be calculated.'.format(
                    category, category))
            return

        relevant_pos = pd.DataFrame()
        self.calculate_fixture_width(relevant_pos, longest_shelf, category)
        return

    def calculate_assortment(self):
        if self.scif.empty or self.store_assortment.empty:
            Log.warning('Unable to calculate assortment: SCIF or store assortment is empty')
            return

        grouped_scif = self.scif.groupby('product_fk', as_index=False)['facings'].sum()
        assortment_with_facings = \
            pd.merge(self.store_assortment, grouped_scif, how='left', on='product_fk')
        assortment_with_facings.loc[:, 'facings'] = assortment_with_facings['facings'].fillna(0)

        for product in assortment_with_facings.itertuples():
            score = 1 if product.facings > 0 else 0
            self.common_v2.write_to_db_result(product.kpi_fk_lvl3, numerator_id=product.product_fk,
                                              denominator_id=product.assortment_fk, numerator_result=product.facings,
                                              result=product.facings, score=score)

        number_of_skus_present = len(assortment_with_facings[assortment_with_facings['facings'] > 0])
        score = 1 if number_of_skus_present > 0 else 0
        kpi_fk = assortment_with_facings['kpi_fk_lvl2'].iloc[0]
        assortment_group_fk = assortment_with_facings['assortment_group_fk'].iloc[0]
        self.common_v2.write_to_db_result(kpi_fk, numerator_id=assortment_group_fk,
                                          numerator_result=number_of_skus_present,
                                          denominator_result=len(assortment_with_facings),
                                          result=number_of_skus_present,
                                          score=score)

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
        relevant_scif = self.scif[self.scif['brand_name'].isin(['Age Verification'])]
        if relevant_scif.empty:
            result = 0
            product_fk = 0
        else:
            result = 1
            product_fk = relevant_scif['product_fk'].iloc[0]

        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Age Verification')
        self.common_v2.write_to_db_result(kpi_fk, numerator_id=product_fk, denominator_id=self.store_id,
                                          result=result)

    def calculate_category_space(self, kpi_set_fk, kpi_name, category, scene_types=None):
        template = self.all_template_data.loc[(self.all_template_data['KPI Level 2 Name'] == kpi_name) &
                                              (self.all_template_data['Value1'] == category)]
        kpi_template = template.loc[template['KPI Level 2 Name'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        values_to_check = []

        filters = {'template_name': scene_types, 'category': kpi_template['Value1']}

        if kpi_template['Value1'] in CATEGORIES:
            category_att = 'category'

        if kpi_template['Value1']:
            values_to_check = self.all_products.loc[self.all_products[category_att] == kpi_template['Value1']][
                category_att].unique().tolist()

        for primary_filter in values_to_check:
            filters[kpi_template['Param1']] = primary_filter

            new_kpi_name = self.kpi_name_builder(kpi_name, **filters)

            result = self.calculate_category_space_length(new_kpi_name,
                                                          **filters)
            filters['Category'] = kpi_template['KPI Level 2 Name']
            score = result
            numerator_id = self.products['category_fk'][self.products['category'] == kpi_template['Value1']].iloc[0]
            self.common_v2.write_to_db_result(kpi_set_fk, numerator_id=numerator_id, numerator_result=999, result=score,
                                              score=score)

    def calculate_category_space_length(self, kpi_name, threshold=0.5, retailer=None, exclude_pl=False, **filters):
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
                scene_matches = self.mpis[self.mpis['scene_fk'] == scene]
                scene_filters = filters
                scene_filters['scene_fk'] = scene
                for bay in scene_matches['bay_number'].unique().tolist():
                    bay_total_linear = scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                         (scene_matches['stacking_layer'] == 1) &
                                                         (scene_matches['status'] == 1)]['width_mm_advance'].sum()
                    scene_filters['bay_number'] = bay

                    tested_group_linear = scene_matches[self.get_filter_condition(scene_matches, **scene_filters)]

                    tested_group_linear_value = tested_group_linear['width_mm_advance'].sum()

                    if tested_group_linear_value:
                        bay_ratio = tested_group_linear_value / float(bay_total_linear)
                    else:
                        bay_ratio = 0

                    if bay_ratio >= threshold:
                        category = filters['category']
                        max_facing = scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                       (scene_matches['stacking_layer'] == 1)][
                            'facing_sequence_number'].max()
                        shelf_length = self.spacing_template_data.query('Category == "' + category +
                                                                        '" & Low <= "' + str(
                            max_facing) + '" & High >= "' + str(max_facing) + '"')
                        shelf_length = int(shelf_length['Size'].iloc[-1])
                        bay_values.append(shelf_length)
                        space_length += shelf_length
        except Exception as e:
            Log.info('Linear Feet calculation failed due to {}'.format(e))
            space_length = 0

        return space_length

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

    def calculate_signage_locations_and_widths(self, category):
        excluded_types = ['Other', 'Irrelevant', 'Empty']
        relevant_scif = self.scif[self.scif['template_name'] == 'Tobacco Merchandising Space']
        if relevant_scif.empty:
            Log.info('No products found for {} category'.format(category))
            return
        # need to include scene_id from previous relevant_scif
        # also need to split this shit up into different categories, i.e. smokeless, cigarettes
        # need to figure out how to deal with POS from smokeless being included with cigarette MPIS

        # get relevant SKUs from the cigarettes category
        relevant_scif = relevant_scif[relevant_scif['category'].isin([category, 'POS'])]
        relevant_product_pks = relevant_scif[relevant_scif['product_type'] == 'SKU']['product_fk'].unique().tolist()
        relevant_pos_pks = \
            relevant_scif[(relevant_scif['product_type'] == 'POS') &
                          ~(relevant_scif['brand_name'] == 'Age Verification') &
                          ~(relevant_scif['product_name'] == 'General POS Other')]['product_fk'].unique().tolist()
        other_product_and_pos_pks = \
            relevant_scif[relevant_scif['product_type'].isin(excluded_types)]['product_fk'].tolist()
        relevant_scene_id = self.get_most_frequent_scene(relevant_scif)
        product_mpis = self.mpis[(self.mpis['product_fk'].isin(relevant_product_pks)) &
                                 (self.mpis['scene_fk'] == relevant_scene_id)]

        if product_mpis.empty:
            Log.info('No products found for {} category'.format(category))
            return

        self.calculate_total_shelves(product_mpis, category)

        longest_shelf = \
            product_mpis[product_mpis['shelf_number'] ==
                         self.get_longest_shelf_number(product_mpis)].sort_values(by='rect_x', ascending=True)

        if longest_shelf.empty or longest_shelf.isnull().all().all():
            Log.warning(
                'The {} category items are in a non-standard location. The {} category will not be calculated.'.format(
                    category, category))
            return

        # demarcation_line = longest_shelf['rect_y'].median() old method, had bugs due to longest shelf being lower
        demarcation_line = product_mpis['rect_y'].min()

        exclusion_line = -9999
        excluded_mpis = self.mpis[~(self.mpis['product_fk'].isin(relevant_pos_pks +
                                                                 relevant_product_pks +
                                                                 other_product_and_pos_pks)) &
                                  (self.mpis['rect_x'] < longest_shelf['rect_x'].max()) &
                                  (self.mpis['rect_x'] > longest_shelf['rect_x'].min()) &
                                  (self.mpis['scene_fk'] == relevant_scene_id) &
                                  (self.mpis['rect_y'] < demarcation_line)]
        # we need this line for when SCIF and MPIS don't match
        excluded_mpis = excluded_mpis[~excluded_mpis['product_type'].isin(excluded_types)]

        if not excluded_mpis.empty:
            exclusion_line = excluded_mpis['rect_y'].max()

        # we need to get POS stuff that falls within the x-range of the longest shelf (which is limited by category)
        # we also need to account for the fact that the images suck, so we're going to add/subtract 5% of the
        # max/min values to allow for POS items that fall slightly out of the shelf length range
        correction_factor = 0.05
        correction_value = (longest_shelf['rect_x'].max() - longest_shelf['rect_x'].min()) * correction_factor
        pos_mpis = self.mpis[(self.mpis['product_fk'].isin(relevant_pos_pks)) &
                             (self.mpis['rect_x'] < (longest_shelf['rect_x'].max() + correction_value)) &
                             (self.mpis['rect_x'] > (longest_shelf['rect_x'].min() - correction_value)) &
                             (self.mpis['scene_fk'] == relevant_scene_id)]

        # DO NOT SET TO TRUE WHEN DEPLOYING
        # debug flag displays polygon_mask graph DO NOT SET TO TRUE WHEN DEPLOYING
        # DO NOT SET TO TRUE WHEN DEPLOYING
        relevant_pos = self.adp.get_products_contained_in_displays(pos_mpis, y_axis_threshold=35, debug=False)

        if relevant_pos.empty:
            Log.warning('No polygon mask was generated for {} category - cannot compute KPIs'.format(category))
            # we need to attempt to calculate fixture width, even if there's no polygon mask
            self.calculate_fixture_width(relevant_pos, longest_shelf, category)
            return

        relevant_pos = relevant_pos[['product_fk', 'product_name', 'left_bound', 'right_bound', 'center_x', 'center_y']]
        relevant_pos = relevant_pos.reindex(columns=relevant_pos.columns.tolist() + ['type', 'width', 'position'])
        relevant_pos['width'] = \
            relevant_pos.apply(lambda row: self.get_length_of_pos(row, longest_shelf, category), axis=1)
        relevant_pos['type'] = \
            relevant_pos['center_y'].apply(lambda x: 'Header' if exclusion_line < x < demarcation_line else 'Flip Sign')
        relevant_pos = relevant_pos.sort_values(['center_x'], ascending=True)
        relevant_pos = self.remove_duplicate_pos_tags(relevant_pos)
        # generate header positions
        if category == 'Cigarettes':
            number_of_headers = len(relevant_pos[relevant_pos['type'] == 'Header'])
            if number_of_headers > len(self.header_positions_template['Cigarettes Positions'].dropna()):
                Log.warning('Number of Headers for Cigarettes is greater than max number defined in template!')
            elif number_of_headers > 0:
                header_position_list = [position.strip() for position in
                                        self.header_positions_template[
                                            self.header_positions_template['Number of Headers'] ==
                                            number_of_headers]['Cigarettes Positions'].iloc[0].split(',')]
                relevant_pos.loc[relevant_pos['type'] == 'Header', ['position']] = header_position_list
        elif category == 'Smokeless':
            relevant_pos = self.check_menu_scene_recognition(relevant_pos)
            number_of_headers = len(relevant_pos[relevant_pos['type'] == 'Header'])
            if number_of_headers > len(self.header_positions_template['Smokeless Positions'].dropna()):
                Log.warning('Number of Headers for Smokeless is greater than max number defined in template!')
            elif number_of_headers > 0:
                header_position_list = [position.strip() for position in
                                        self.header_positions_template[
                                            self.header_positions_template['Number of Headers'] ==
                                            number_of_headers]['Smokeless Positions'].iloc[0].split(',')]
                relevant_pos.loc[relevant_pos['type'] == 'Header', ['position']] = header_position_list

            relevant_pos = self.get_menu_board_items(relevant_pos, longest_shelf, pos_mpis)
        # generate flip-sign positions
        if category == 'Cigarettes':
            relevant_template = \
                self.fixture_width_template.loc[(self.fixture_width_template['Fixture Width (facings)']
                                                 - len(longest_shelf)).abs().argsort()[:1]].dropna(axis=1)
            locations = relevant_template.columns[2:].tolist()
            right_bound = 0
            longest_shelf_copy = longest_shelf.copy()
            for location in locations:
                if right_bound > 0:
                    left_bound = right_bound + 1
                else:
                    left_bound = longest_shelf_copy.iloc[:relevant_template[location].iloc[0]]['rect_x'].min()
                right_bound = longest_shelf_copy.iloc[:relevant_template[location].iloc[0]]['rect_x'].max()
                if locations[-1] == location:
                    right_bound = right_bound + abs(right_bound * 0.05)
                flip_sign_pos = relevant_pos[(relevant_pos['type'] == 'Flip Sign') &
                                             (relevant_pos['center_x'] > left_bound) &
                                             (relevant_pos['center_x'] < right_bound)]
                if flip_sign_pos.empty:
                    # add 'NO Flip Sign' product_fk
                    relevant_pos.loc[len(relevant_pos), ['position', 'product_fk', 'type']] = \
                        [location, NO_FLIP_SIGN_PK, 'Flip Sign']
                else:
                    relevant_pos.loc[flip_sign_pos.index, ['position']] = location
                longest_shelf_copy.drop(longest_shelf_copy.iloc[:relevant_template[location].iloc[0]].index,
                                        inplace=True)
        elif category == 'Smokeless':
            # if there are no flip signs found, there are no positions to assign
            number_of_flip_signs = len(relevant_pos[relevant_pos['type'] == 'Flip Sign'])
            if number_of_flip_signs > self.flip_sign_positions_template['Number of Flip Signs'].max():
                Log.warning('Number of Flip Signs for Smokeless is greater than max number defined in template!')
            elif number_of_flip_signs > 0:
                flip_sign_position_list = [position.strip() for position in
                                           self.flip_sign_positions_template[
                                               self.flip_sign_positions_template['Number of Flip Signs'] ==
                                               number_of_flip_signs]['Position'].iloc[0].split(',')]
                relevant_pos.loc[relevant_pos['type'] == 'Flip Sign', ['position']] = flip_sign_position_list

            # store empty flip sign values
            for location in ['Secondary', 'Tertiary']:
                if location not in relevant_pos[relevant_pos['type'] == 'Flip Sign']['position'].tolist():
                    relevant_pos.loc[len(relevant_pos), ['position', 'product_fk', 'type']] = \
                        [location, NO_FLIP_SIGN_PK, 'Flip Sign']

        relevant_pos = relevant_pos.reindex(columns=relevant_pos.columns.tolist() + ['denominator_id'])

        # this is a bandaid fix that should be removed ->  'F7011A7C-1BB6-4007-826D-2B674BD99DAE'
        # removes POS items that were 'extra', i.e. more than max value in template
        # only affects smokeless
        relevant_pos.dropna(subset=['position'], inplace=True)

        relevant_pos.loc[:, ['denominator_id']] = relevant_pos['position'].apply(self.get_custom_entity_pk)

        for row in relevant_pos.itertuples():
            kpi_fk = self.common_v2.get_kpi_fk_by_kpi_name(row.type)
            self.common_v2.write_to_db_result(kpi_fk, numerator_id=row.product_fk, denominator_id=row.denominator_id,
                                              result=row.width, score=row.width)

        self.calculate_fixture_width(relevant_pos, longest_shelf, category)
        return

    def calculate_total_shelves(self, longest_shelf, category, product_mpis=None):
        category_fk = self.get_category_fk_by_name(category)
        if product_mpis is None:
            product_mpis = self.mpis[(self.mpis['rect_x'] > longest_shelf['rect_x'].min()) &
                                     (self.mpis['rect_x'] < longest_shelf['rect_x'].max()) &
                                     (self.mpis['scene_fk'] == longest_shelf['scene_fk'].fillna(0).mode().iloc[0])]
        total_shelves = len(product_mpis['shelf_number'].unique())

        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_name('Total Shelves')
        self.common_v2.write_to_db_result(kpi_fk, numerator_id=category_fk, denominator_id=self.store_id,
                                          result=total_shelves)

    def calculate_fixture_width(self, relevant_pos, longest_shelf, category):
        longest_shelf = longest_shelf[longest_shelf['stacking_layer'] == 1]
        category_fk = self.get_category_fk_by_name(category)
        # this is needed to remove intentionally duplicated 'Menu Board' POS 'Headers'
        relevant_pos = relevant_pos.drop_duplicates(subset=['position'])
        try:
            width = relevant_pos[relevant_pos['type'] == 'Header']['width'].sum()
        except KeyError:
            # needed for when 'width' doesn't exist
            width = 0

        if relevant_pos.empty or width == 0:
            width = round(len(longest_shelf) / float(self.facings_to_feet_template[category + ' Facings'].iloc[0]))

        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_name('Fixture Width')
        self.common_v2.write_to_db_result(kpi_fk, numerator_id=category_fk, denominator_id=self.store_id,
                                          result=width)

    def get_category_fk_by_name(self, category_name):
        return self.all_products[self.all_products['category'] == category_name]['category_fk'].iloc[0]

    def check_menu_scene_recognition(self, relevant_pos):
        mdis = self.adp.get_match_display_in_scene()
        mdis = mdis[mdis['display_name'] == 'Menu POS']

        if mdis.empty:
            return relevant_pos

        dummy_sku_for_menu_pk = 9282  # 'Other (Smokeless Tobacco)'
        dummy_sky_for_menu_name = 'Menu POS (Scene Recognized Item)'
        location_type = 'Header'
        width = 1
        center_x = mdis['x'].iloc[0]
        center_y = mdis['y'].iloc[0]
        relevant_pos.loc[len(relevant_pos), ['product_fk', 'product_name', 'center_x', 'center_y', 'type', 'width']] = \
            [dummy_sku_for_menu_pk, dummy_sky_for_menu_name, center_x, center_y, location_type, width]
        relevant_pos = relevant_pos.sort_values(['center_x'], ascending=True).reset_index(drop=True)
        return relevant_pos

    def get_menu_board_items(self, relevant_pos, longest_shelf, pos_mpis):
        # get the placeholder item
        menu_board_dummy = relevant_pos[relevant_pos['product_name'] == 'Menu POS (Scene Recognized Item)']

        # if no placeholder, this function isn't relevant
        if menu_board_dummy.empty:
            return relevant_pos

        center_x = menu_board_dummy['center_x'].iloc[0]
        center_y = menu_board_dummy['center_y'].iloc[0]
        position = menu_board_dummy['position'].iloc[0]

        demarcation_line = longest_shelf['rect_y'].min()
        upper_demarcation_line = center_y - (demarcation_line - center_y)

        distance_in_facings = 2

        try:
            left_bound = longest_shelf[longest_shelf['rect_x'] < center_x].sort_values(
                by=['rect_x'], ascending=False)['rect_x'].iloc[int(distance_in_facings) - 1]
        except IndexError:
            # if there are no POS items found to the left of the 'Menu POS' scene recognition tag, use the tag itself
            # in theory this should never happen
            left_bound = center_x

        try:
            right_bound = longest_shelf[longest_shelf['rect_x'] > center_x].sort_values(
                by=['rect_x'], ascending=True)['rect_x'].iloc[int(distance_in_facings) - 1]
        except IndexError:
            # if there are no POS items found to the right of the 'Menu POS' scene recognition tag, use the tag itself
            # this is more likely to happen for the right bound than the left bound
            right_bound = center_x

        pos_mpis = pos_mpis[(pos_mpis['rect_x'] > left_bound) &
                            (pos_mpis['rect_x'] < right_bound) &
                            (pos_mpis['rect_y'] > upper_demarcation_line) &
                            (pos_mpis['rect_y'] < demarcation_line)]

        if pos_mpis.empty:
            return relevant_pos

        # remove the placeholder item
        relevant_pos = relevant_pos[~(relevant_pos['product_name'] == 'Menu POS (Scene Recognized Item)')]

        location_type = 'Header'
        width = 1

        for row in pos_mpis.itertuples():
            relevant_pos.loc[len(relevant_pos), ['product_fk', 'product_name', 'center_x',
                                                 'center_y', 'type', 'width', 'position']] = \
                [row.product_fk, row.product_name, row.rect_x, row.rect_y, location_type, width, position]

        return relevant_pos

    @staticmethod
    def remove_duplicate_pos_tags(relevant_pos_df):
        duplicate_results = \
            relevant_pos_df[relevant_pos_df.duplicated(subset=['left_bound', 'right_bound'], keep=False)]

        duplicate_results_without_other = duplicate_results[~duplicate_results['product_name'].str.contains('Other')]

        results_without_duplicates = \
            relevant_pos_df[~relevant_pos_df.duplicated(subset=['left_bound', 'right_bound'], keep=False)]

        if duplicate_results_without_other.empty:
            return relevant_pos_df[~relevant_pos_df.duplicated(subset=['left_bound', 'right_bound'], keep='first')]
        else:
            results = pd.concat([duplicate_results_without_other, results_without_duplicates])
            # we need to sort_index to fix the sort order to reflect center_x values
            return results.drop_duplicates(subset=['left_bound', 'right_bound']).sort_index()

    def get_custom_entity_pk(self, name):
        return self.custom_entity_data[self.custom_entity_data['name'] == name]['pk'].iloc[0]

    def get_length_of_pos(self, row, longest_shelf, category):
        width_in_facings = len(longest_shelf[(longest_shelf['rect_x'] > row['left_bound']) &
                                             (longest_shelf['rect_x'] < row['right_bound'])]) + 0.75
        category_facings = category + ' Facings'
        return self.facings_to_feet_template.loc[(self.facings_to_feet_template[category_facings]
                                                  - width_in_facings).abs().argsort()[:1]]['POS Width (ft)'].iloc[0]

    @staticmethod
    def get_longest_shelf_number(relevant_mpis, max_shelves_from_top=3):
        # returns the shelf_number of the longest shelf
        try:
            longest_shelf = \
                relevant_mpis[relevant_mpis['shelf_number'] <= max_shelves_from_top].groupby('shelf_number').agg(
                    {'scene_match_fk': 'count'})['scene_match_fk'].idxmax()
        except ValueError:
            longest_shelf = pd.DataFrame()

        return longest_shelf

    @staticmethod
    def get_most_frequent_scene(relevant_scif):
        try:
            relevant_scene_id = relevant_scif['scene_id'].fillna(0).mode().iloc[0]
        except IndexError:
            relevant_scene_id = 0
        return relevant_scene_id

    def commit(self):
        self.common_v2.commit_results_data()
