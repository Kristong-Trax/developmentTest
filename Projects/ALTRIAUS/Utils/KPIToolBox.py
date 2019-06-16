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

        self.kpi_new_static_data = self.common.get_new_kpi_static_data()
        self.mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
                    .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
                      .merge(self.template_info, on='template_fk', suffixes=['', '_t'])
        self.adp = AltriaDataProvider(self.data_provider)

    def main_calculation(self, *args, **kwargs):
        """
               This function calculates the KPI results.
               """
        self.calculate_signage_locations_and_widths('Cigarettes')
        self.calculate_signage_locations_and_widths('Smokeless')

        return

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
            self.common.write_to_db_result_new_tables(kpi_set_fk, numerator_id, 999, score, score=score)

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
        relevant_scif = self.scif[self.scif['template_name'] == 'Tobacco Merchandising Space']
        # need to include scene_id from previous relevant_scif
        # also need to split this shit up into different categories, i.e. smokeless, cigarettes
        # need to figure out how to deal with POS from smokeless being included with cigarette MPIS

        # get relevant SKUs from the cigarettes category
        relevant_scif = relevant_scif[relevant_scif['category'].isin([category, 'POS'])]
        relevant_product_pks = relevant_scif[relevant_scif['product_type'] == 'SKU']['product_fk'].unique().tolist()
        relevant_pos_pks = relevant_scif[relevant_scif['product_type'] == 'POS']['product_fk'].unique().tolist()
        product_mpis = self.mpis[self.mpis['product_fk'].isin(relevant_product_pks)]

        if product_mpis.empty:
            Log.info('No products found for {} category'.format(category))
            return

        longest_shelf = \
            product_mpis[product_mpis['shelf_number'] ==
                         self.get_longest_shelf_number(product_mpis)].sort_values(by='rect_x', ascending=True)
        demarcation_line = longest_shelf['rect_y'].median()

        # we need to get POS stuff that falls within the x-range of the longest shelf (which is limited by category)
        # we also need to account for the fact that the images suck, so we're going to add/subtract 5% of the
        # max/min values to allow for POS items that fall slightly out of the shelf length range
        pos_mpis = self.mpis[(self.mpis['product_fk'].isin(relevant_pos_pks)) &
                             (self.mpis['rect_x'] < (longest_shelf['rect_x'].max() * 1.05)) &
                             (self.mpis['rect_x'] > (longest_shelf['rect_x'].min() * 0.95))]
        relevant_pos = self.adp.get_products_contained_in_displays(pos_mpis)
        relevant_pos = relevant_pos[['product_fk', 'product_name', 'left_bound', 'right_bound', 'center_x', 'center_y']]
        relevant_pos = relevant_pos.reindex(columns=relevant_pos.columns.tolist() + ['type', 'width', 'position'])
        relevant_pos['width'] = relevant_pos.apply(lambda row: self.get_length_of_pos(row, longest_shelf, category), axis=1)
        relevant_pos['type'] = relevant_pos['center_y'].apply(lambda x: 'Header' if x < demarcation_line else 'Flip Sign')
        relevant_pos = relevant_pos.sort_values(['center_x'], ascending=True)
        relevant_pos = self.remove_duplicate_pos_tags(relevant_pos)
        # generate header positions
        if category == 'Cigarettes':
            header_position_list = [position.strip() for position in
                                    self.header_positions_template[
                                        self.header_positions_template['Number of Headers'] ==
                                        len(relevant_pos[relevant_pos['type'] ==
                                                         'Header'])]['Cigarettes Positions'].iloc[0].split(',')]
            relevant_pos.loc[relevant_pos['type'] == 'Header', ['position']] = header_position_list
        elif category == 'Smokeless':
            header_position_list = [position.strip() for position in
                                    self.header_positions_template[
                                        self.header_positions_template['Number of Headers'] ==
                                        len(relevant_pos[relevant_pos['type'] ==
                                                         'Header'])]['Smokeless Positions'].iloc[0].split(',')]
            relevant_pos.loc[relevant_pos['type'] == 'Header', ['position']] = header_position_list
        # generate flip-sign positions
        if category == 'Cigarettes':
            relevant_template = \
                self.fixture_width_template.loc[(self.fixture_width_template['Fixture Width (facings)']
                                                 - len(longest_shelf)).abs().argsort()[:1]].dropna(axis=1)
            locations = relevant_template.columns[2:].tolist()
            for location in locations:
                left_bound = longest_shelf.iloc[:relevant_template[location].iloc[0]]['rect_x'].min()
                right_bound = longest_shelf.iloc[:relevant_template[location].iloc[0]]['rect_x'].max()
                if locations[-1] == location:
                    right_bound = right_bound * 1.05
                flip_sign_pos = relevant_pos[(relevant_pos['type'] == 'Flip Sign') &
                                             (relevant_pos['center_x'] > left_bound) &
                                             (relevant_pos['center_x'] < right_bound)]
                if flip_sign_pos.empty:
                    # pass
                    # need to add 'No Flip-Sign' product_fk too
                    relevant_pos.loc[len(relevant_pos), ['position']] = location
                else:
                    relevant_pos.loc[flip_sign_pos.index, ['position']] = location
                longest_shelf.drop(longest_shelf.iloc[:relevant_template[location].iloc[0]].index, inplace=True)
        elif category == 'Smokeless':
            flip_sign_position_list = [position.strip() for position in
                                       self.flip_sign_positions_template[
                                           self.flip_sign_positions_template['Number of Flip Signs'] ==
                                           len(relevant_pos[relevant_pos['type'] ==
                                                            'Flip Sign'])]['Position'].iloc[0].split(',')]
            relevant_pos.loc[relevant_pos['type'] == 'Flip Sign', ['position']] = flip_sign_position_list

        relevant_pos = relevant_pos.reindex(columns=relevant_pos.columns.tolist() + ['denominator_id'])

        #this is a bandaid fix that should be removed ->  'F7011A7C-1BB6-4007-826D-2B674BD99DAE'
        relevant_pos.dropna(subset=['position'], inplace=True)

        relevant_pos.loc[:, ['denominator_id']] = relevant_pos['position'].apply(self.get_custom_entity_pk)

        # remove this after 'No Flip-Sign' SKU is added
        relevant_pos.dropna(subset=['product_fk'], inplace=True)

        for row in relevant_pos.itertuples():
            kpi_fk = self.common_v2.get_kpi_fk_by_kpi_name(row.type)
            self.common_v2.write_to_db_result(kpi_fk, numerator_id=row.product_fk, denominator_id=row.denominator_id,
                                              result=row.width, score=row.width)
        return

    def remove_duplicate_pos_tags(self, relevant_pos_df):
        duplicate_results = \
            relevant_pos_df[relevant_pos_df.duplicated(subset=['left_bound', 'right_bound'], keep=False)]
        duplicate_results_without_other = duplicate_results[~duplicate_results['product_name'].str.contains('Other')]
        if duplicate_results_without_other.empty:
            return relevant_pos_df[~relevant_pos_df.duplicated(subset=['left_bound', 'right_bound'], keep='first')]
        else:
            results_without_duplicates = \
                relevant_pos_df[~relevant_pos_df.duplicated(subset=['left_bound', 'right_bound'], keep=False)]
            return pd.concat([duplicate_results_without_other, results_without_duplicates])

    def get_custom_entity_pk(self, name):
        return self.custom_entity_data[self.custom_entity_data['name'] == name]['pk'].iloc[0]

    def get_length_of_pos(self, row, longest_shelf, category):
        width_in_facings = len(longest_shelf[(longest_shelf['rect_x'] > row['left_bound']) &
                                             (longest_shelf['rect_x'] < row['right_bound'])])
        category_facings = category + ' Facings'
        return self.facings_to_feet_template.loc[(self.facings_to_feet_template[category_facings]
                                                  - width_in_facings).abs().argsort()[:1]]['POS Width (ft)'].iloc[0]

    def get_longest_shelf_number(self, relevant_mpis):
        # returns the shelf_number of the longest shelf
        return \
        relevant_mpis[relevant_mpis['shelf_number'] <= 3].groupby('shelf_number').agg({'scene_match_fk': 'count'})[
            'scene_match_fk'].idxmax()

    def commit(self):
        # self.common.commit_results_data_to_new_tables()
        self.common_v2.commit_results_data()
