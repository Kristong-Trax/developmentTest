import os
import pandas as pd
from datetime import datetime

from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Projects.PNGAMERICA.Utils.ParseTemplates import parse_template
from Projects.PNGAMERICA.Utils.Fetcher import PNGAMERICAQueries
from Projects.PNGAMERICA.Utils.GeneralToolBox import PNGAMERICAGENERALToolBox
from Projects.PNGAMERICA.Utils.AutoAssortment import AutoAssortmentHandler

__author__ = 'Ortal'

BINARY = 'binary'
NUMBER = 'number'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
NUMERATOR = 'Numerator'
DENOMINATOR = 'Denominator'
ENTITY = 'Entity'
OSA = 'OSA'
DVOID = 'D-VOID'
BLOCK_KPI_NAME = 'Blocking:Prod_lvl_Blocking:{category}:BRAND={brand_name}'
CSPACE_KPI_NAME = 'Spacing:Category_Space:{category}'
BLOCK_KPI_NAMES_PAIRING_MAPPING = {
    'Blocking:Prod_lvl_Blocking:{category}:BRAND={brand_name}':
        'Blocking:Block_Hor_vs_Ver:{category}:BRAND={brand_name}',
    'Blocking:Prod_lvl_Blocking:{category}:MFG={manufacturer_name}':
        'Blocking:Block_Hor_vs_Ver:{category}:MFG={manufacturer_name}',
    'Blocking:Prod_lvl_Blocking:{category}:SEG={SEGMENT}': "Blocking:Block_Hor_vs_Ver:{category}:SEG={SEGMENT}",
    'Blocking:Prod_lvl_Blocking:{category}:BRAND={brand_name}:SEG={SEGMENT}':
        'Blocking:Block_Hor_vs_Ver:{category}:BRAND={brand_name}:SEG={SEGMENT}',
    'Blocking:Prod_lvl_Blocking:{category}:SBRAND={Sub Brand}:SEG={SEGMENT}':
        'Blocking:Block_Hor_vs_Ver:{category}:SBRAND={Sub Brand}:SEG={SEGMENT}',
    'Blocking:Prod_lvl_Blocking:{category}:SEG={SEGMENT}:FORM={FORM}':
        'Blocking:Block_Hor_vs_Ver:{category}:SEG={SEGMENT}:FORM={FORM}'}
CATEGORY_OSA_MAPPING = {
    'AIR CARE': 'OSA AIR CARE',
    'AP/DO': 'OSA AP/DO',
    'BABY CARE': 'OSA BABY CARE',
    'DISH CARE': 'OSA DISH CARE',
    'DIGESTIVE': 'OSA DIGESTIVE',
    'FABRICARE': 'OSA FABRICARE',
    'FEMININE CARE / AI': 'OSA FEMININE CARE / AI',
    'FAMILY CARE PAPER PRODUCTS': 'OSA FAMILY CARE PAPER PRODUCTS',
    'HAIR CARE': 'OSA HAIR CARE',
    'ORAL CARE': 'OSA ORAL CARE',
    'PERSONAL CLEANSING': 'OSA PERSONAL CLEANSING',
    'QUICK CLEAN': 'OSA QUICK CLEAN',
    'RESPIRATORY': 'OSA RESPIRATORY',
    'SHAVE': 'OSA SHAVE',
    'SKIN CARE': 'OSA SKIN CARE'
}
CATEGORY_DVOID_MAPPING = {
    'AIR CARE': 'D-VOID AIR CARE',
    'AP/DO': 'D-VOID AP/DO',
    'BABY CARE': 'D-VOID BABY CARE',
    'DISH CARE': 'D-VOID DISH CARE',
    'DIGESTIVE': 'D-VOID DIGESTIVE',
    'FABRICARE': 'D-VOID FABRICARE',
    'FEMININE CARE / AI': 'D-VOID FEMININE CARE / AI',
    'FAMILY CARE PAPER PRODUCTS': 'D-VOID FAMILY CARE PAPER PRODUCTS',
    'HAIR CARE': 'D-VOID HAIR CARE',
    'ORAL CARE': 'D-VOID ORAL CARE',
    'PERSONAL CLEANSING': 'D-VOID PERSONAL CLEANSING',
    'QUICK CLEAN': 'D-VOID QUICK CLEAN',
    'RESPIRATORY': 'D-VOID RESPIRATORY',
    'SHAVE': 'D-VOID SHAVE',
    'SKIN CARE': 'D-VOID SKIN CARE'
}
BLOCK_PARAMS = ['SEGMENT', 'sub_category', 'brand_name', 'SUPER CATEGORY', 'manufacturer_name', 'NATURALS', 'Sub Brand',
                'P&G BRAND', 'PRIVATE_LABEL', 'GENDER', 'PRICE SEGMENT', 'HEAD SIZE', 'PG SIZE']
ADJACENCY_PARAMS = ['sub_category', 'brand_name', 'NATURALS', 'Sub Brand',
                    'P&G BRAND', 'BENEFIT', 'AUDIENCE', 'PRICE SEGMENT', 'FORM', 'HEAD SIZE']
BLOCK_TOGETHER = ['Regular Block', 'horizontally blocked', 'vertically blocked', 'Orphan products', 'group in block',
                  'regular block', 'block in block', 'hor_vs_vertical']
FABRICARE_CATEGORIES = ['TOTAL FABRIC CONDITIONERS', 'BLEACH AND LAUNDRY ADDITIVES', 'TOTAL LAUNDRY CARE']
PG_CATEGORY = 'PG_CATEGORY'
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template_v4.3.xlsx')
POWER_SKUS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'PowerSKUs_3.xlsx')

GOLDEN_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'golden_shelves.xlsx'
                           )


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


class PNGAMERICAToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    MM_TO_FEET_CONVERSION = 0.0032808399

    def __init__(self, data_provider, output):
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
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.scif['Sub Brand'] = self.scif['Sub Brand'].str.strip()
        self.tools = PNGAMERICAGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.retailer = self.get_store_retailer(self.store_id)
        self.kpi_results_queries = []
        self.custom_templates = {}
        self.all_template_data = parse_template(TEMPLATE_PATH, 'main')
        self.block_data = parse_template(TEMPLATE_PATH, 'block new')
        self.anchor_data = parse_template(TEMPLATE_PATH, 'anchor new')
        self.orchestrated_data = parse_template(TEMPLATE_PATH, 'orchestrated new')
        self.adjacency_data = parse_template(TEMPLATE_PATH, 'adjacency new')
        self.count_of_data = parse_template(TEMPLATE_PATH, 'count')
        self.linear_feet_data = parse_template(TEMPLATE_PATH, 'linear feet new')
        # self.availability_data = parse_template(TEMPLATE_PATH, 'availability')
        # self.flexible_block = parse_template(TEMPLATE_PATH, 'flexible blocks')
        self.relative_position = parse_template(TEMPLATE_PATH, 'relative')
        self.checkerboarded_template = parse_template(TEMPLATE_PATH, 'checkerboarded new')
        self.eye_level_data = parse_template(TEMPLATE_PATH, 'eye level new')
        self.posm_data = parse_template(TEMPLATE_PATH, 'posm')
        self.block_and_availability_data = parse_template(TEMPLATE_PATH, 'block and availability')
        self.average_shelf = parse_template(TEMPLATE_PATH, 'average shelf')
        # self.sos_template = parse_template(TEMPLATE_PATH, 'sos')
        # self.pantene_template = parse_template(TEMPLATE_PATH, 'pantene')
        # self.hns_template = parse_template(TEMPLATE_PATH, 'H&S')
        # self.he_template = parse_template(TEMPLATE_PATH, 'HE')
        # self.additional_display_data = self.get_additional_display_per_session_data()
        # self.eye_level_definition = parse_template(GOLDEN_PATH)
        self.category_space_data = parse_template(TEMPLATE_PATH, 'category space')
        self.bookend_data = parse_template(TEMPLATE_PATH, 'bookend')
        self.eye_level_definition = pd.read_excel(GOLDEN_PATH, 'default')
        self.power_skus = parse_template(POWER_SKUS_PATH)
        self.eye_level_mapping = {'ORAL CARE': pd.read_excel(GOLDEN_PATH, 'ORAL CARE'),
                                  'FABRICARE': pd.read_excel(GOLDEN_PATH, 'FABRICARE'),
                                  'AI': pd.read_excel(GOLDEN_PATH, 'AI'),
                                  'FEM CARE': pd.read_excel(GOLDEN_PATH, 'FEM CARE')}
        self.related_kpi_results = {}
        self.block_results = {}
        self.ps_dataprovider = PsDataProvider(data_provider, output)
        self.scif = self._filter_excluded_scene()
        self.eye_level_results = {}
        self.hor_ver_results = {}

    def _filter_excluded_scene(self):
        excluded_scenes_df = self.ps_dataprovider.get_excluded_scenes()
        mask = self.scif['scene_id'].isin(excluded_scenes_df['pk'])
        return self.scif[~mask]

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = PNGAMERICAQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_sub_category_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = PNGAMERICAQueries.get_sub_category_data()
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        sub_category_data = pd.read_sql_query(query, self.rds_conn.db)
        self.scif = self.scif.merge(sub_category_data, how='left', on='sub_category_fk')
        return

    def get_additional_display_per_session_data(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
            The data is taken from static.kpi_level_2.
            """
        query = PNGAMERICAQueries.get_additional_display_data(self.session_uid)
        additional_display_data = pd.read_sql_query(query, self.rds_conn.db)
        return additional_display_data

    def get_custom_template(self, template_path, name):
        if name not in self.custom_templates.keys():
            template = parse_template(template_path, sheet_name=name)
            if template.empty:
                template = parse_template(template_path, name, 2)
            self.custom_templates[name] = template
        return self.custom_templates[name]

    def get_store_retailer(self, store_fk):
        query = PNGAMERICAQueries.get_store_retailer(store_fk)
        att10 = pd.read_sql_query(query, self.rds_conn.db)
        return att10.values[0][0]

    def main_calculation(self, kpi_set_fk):
        """
        This function calculates the KPI results.
        """
        block_calc_indication = {}
        self.block_results = {}
        self.tools.average_shelf_values = {}
        set_name = self.kpi_static_data.loc[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].values[0]
        template_data = self.all_template_data.loc[self.all_template_data['kpi set name'] == set_name]
        kpi_list = template_data['KPI name'].tolist()
        try:
            if set_name and not set(template_data['Scene Types to Include'].values[0].encode().split(', ')) & set(
                    self.scif['template_name'].unique().tolist()):
                Log.info('Category {} was not captured'.format(template_data['category'].values[0]))
                return
        except Exception as e:
            Log.info('KPI Set {} is not defined in the template'.format(set_name))
        # for kpi_name in kpi_list:
        for i, row in template_data.iterrows():
            try:
                kpi_name = row['KPI name']
                # kpi_data = template_data.loc[template_data['KPI name'] == kpi_name]
                # scene_type = [s for s in kpi_data['Scene Types to Include'].values[0].encode().split(', ')]
                scene_type = [s for s in row['Scene Types to Include'].encode().split(', ')]
                # kpi_type = kpi_data['KPI Type'].values[0]
                kpi_type = row['KPI Type']

                # category = kpi_data['category'].values[0]
                category = row['category']

                if kpi_type not in ['anchor']:
                    # ['category space', 'orchestrated', 'linear feet', 'count of', 'average shelf']
                    continue

                # if kpi_data['KPI Group type'].values[0]:
                #     if kpi_type in BLOCK_TOGETHER:
                #         if kpi_data['Tested KPI Group'].values[0] not in block_calc_indication.keys():
                #             kpi_data_group = template_data.loc[template_data['Tested KPI Group'] ==
                #                                                kpi_data['Tested KPI Group'].values[0]]
                #             self.calculate_block_together(kpi_set_fk, kpi_data['Tested KPI Group'].values[0],
                #                                           scene_type)
                #             kpis_to_remove = kpi_data_group['KPI name'].unique().tolist()
                #             block_calc_indication[kpi_data['Tested KPI Group'].values[0]] = 1
                #             # for kpi in kpis_to_remove:
                #             #     kpi_list.remove(kpi)
                #     if kpi_type == 'anchor list':
                #         category = kpi_data['category'].values[0]
                #         self.calculate_anchor(kpi_set_fk, kpi_data['Tested KPI Group'].values[0], scene_type, category,
                #                               list_type=True)
                #     if kpi_type == 'adj to list':
                #         self.calculate_adjacency(kpi_set_fk, kpi_data['KPI Group type'].values[0], scene_type,
                #                                  list_type=True)
                #     if kpi_type == 'checkerboarded list':
                #         self.calculate_checkerboarded(kpi_set_fk, kpi_data['Tested KPI Group'].values[0], scene_type,
                #                                       list_type=True)
                #     if kpi_type == 'eye level list':
                #         category = kpi_data['category'].values[0]
                #         self.calculate_eye_level(kpi_set_fk, kpi_data['Tested KPI Group'].values[0], scene_type, category,
                #                               list_type=True)
                if kpi_type in BLOCK_TOGETHER:
                    self.calculate_block_together_new(kpi_set_fk, kpi_name, scene_type, category)
                elif kpi_type == 'availability':
                    self.calculate_availability(kpi_set_fk, kpi_name, scene_type)
                elif kpi_type == 'count of':
                    self.calculate_count_of_new(kpi_set_fk, kpi_name, scene_type, category)
                elif kpi_type == 'linear feet':
                    self.calculate_linear_feet_new(kpi_set_fk, kpi_name, scene_type, category)
                elif kpi_type == 'relative':
                    self.calculate_relative_position(kpi_set_fk, kpi_name, scene_type)
                elif kpi_type == 'checkerboarded':
                    self.calculate_checkerboarded_new(kpi_set_fk, kpi_name, scene_type, category)
                elif kpi_type == 'anchor':
                    self.calculate_anchor_new(kpi_set_fk, kpi_name, scene_type, category)
                elif kpi_type == 'orchestrated':
                    self.calculate_orchestrated_new(kpi_set_fk, kpi_name, scene_type)
                elif kpi_type == 'adj to':
                    self.calculate_adjacency_new(kpi_set_fk, kpi_name, scene_type, category)
                elif kpi_type == 'eye level':
                    self.calculate_eye_level_new(kpi_set_fk, kpi_name, scene_type, category)
                elif kpi_type == 'block and availability':
                    self.calculate_block_and_availability(kpi_set_fk, kpi_name, scene_type)
                elif kpi_type == 'average shelf':
                    self.calculate_average_shelf_new(kpi_set_fk, kpi_name, scene_type, category)
                elif kpi_type == 'pantene':
                    self.pantene_golden_strategy(kpi_set_fk, kpi_name, scene_type)
                # elif kpi_type == 'HE':
                #     self.herbal_essences_color_wheel(kpi_set_fk, kpi_name, scene_type)
                # elif kpi_type == 'H&S':
                #     self.head_and_shoulders_solution_center(kpi_set_fk, kpi_name, scene_type)
                elif kpi_type == 'category space':
                    self.calculate_category_space(kpi_set_fk, kpi_name, scene_type, category)
                elif kpi_type == 'bookend':
                    self.calculate_bookend(kpi_set_fk, kpi_name, scene_type)
                elif kpi_type == 'posm':
                    self.calculate_posm_availability(kpi_set_fk, kpi_name, scene_type)
                elif kpi_type == 'sud_orchestration':
                    self.calculate_sud_orchestration(kpi_set_fk, kpi_name, scene_type)
                elif kpi_type == 'fabricare_regimen':
                    self.calculate_fabricare_regimen(kpi_set_fk, kpi_name, scene_type)
            except Exception as e:
                Log.info('KPI {} calculation failed due to {}'.format(kpi_name.encode('utf-8'), e))
                continue
        return

    @kpi_runtime()
    def calculate_block_and_availability(self, kpi_set_fk, kpi_name, scene_types):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        template = self.block_and_availability_data
        kpi_data = template.loc[template['kpi Name'] == kpi_name]
        score = 0
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        if scene_types:
            relevant_scenes = self.scif[self.scif['template_name'].isin(scene_types)]['scene_id'].unique().tolist()
            conditions = {kpi_data['Block param']: (0, [s for s in kpi_data['Block value'].split(', ')], 2),
                          # 'shelf_number': (0, None, 3),
                          kpi_data['availability param']: (
                              0, [s for s in kpi_data['availability value'].split(', ')], 1)}
            # conditions = {kpi_data['Block param']: [s for s in kpi_data['Block value'].split(', ')],
            #               # 'shelf_number': (0, None, 3),
            #               kpi_data['availability param']: [s for s in kpi_data['availability value'].split(', ')]}
            for scene in relevant_scenes:
                general_filters = {'scene_id': scene}
                result = self.tools.calculate_existence_of_blocks(conditions, **general_filters)
                score += 1 if result else 0
        scores = 1 if score > 0 else 0
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=scores, score=scores)

    def calculate_special_sequence(self, kpi_set_fk, kpi_name, tab, scene_types):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        kpi_data = self.flexible_block.loc[self.flexible_block['KPI name'] == kpi_name]
        score = 0
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        if scene_types:
            general_filters = {'template_name': scene_types}
            if kpi_data['sub_category']:
                general_filters['sub_category'] = kpi_data['sub_category']
            if kpi_data['SEGMENT']:
                general_filters['SEGMENT'] = kpi_data['SEGMENT']
            if kpi_data['Super Category']:
                general_filters['SUPER CATEGORY'] = kpi_data['Super Category']
            dict_result = self.tools.calculate_flexible_blocks(group=True, **general_filters)
            result = sum(dict_result.values())
            score = result if result > 0 else 0
            self.related_kpi_results[kpi_name] = score
            score = 1 if score > 0 else 0
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)

    def calculate_relative_position(self, kpi_set_fk, kpi_name, scene_types, return_result=False):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        kpi_data = self.relative_position.loc[self.relative_position['KPI name'] == kpi_name]
        score = 0
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        if scene_types:
            if ',' in kpi_data['Group 1 value']:
                tested_filters = {kpi_data['Group 1 param']: kpi_data['Group 1 value'].split(', ')}
            else:
                tested_filters = {kpi_data['Group 1 param']: kpi_data['Group 1 value']}
            if kpi_data['Group 1 param 2']:
                if ',' in kpi_data['Group 1 value 2']:
                    tested_filters[kpi_data['Group 1 param 2']] = kpi_data['Group 1 value 2'].split(', ')
                else:
                    tested_filters[kpi_data['Group 1 param 2']] = kpi_data['Group 1 value 2']
            if kpi_data['Group 1 param 3']:
                if ',' in kpi_data['Group 1 value 3']:
                    tested_filters[kpi_data['Group 1 param 3']] = kpi_data['Group 1 value 3'].split(', ')
                else:
                    tested_filters[kpi_data['Group 1 param 3']] = kpi_data['Group 1 value 3']
            anchor_filters = {kpi_data['Group 2 param']: kpi_data['Group 2 value']}
            if kpi_data['Group 2 param 2']:
                if ',' in kpi_data['Group 1 value 2']:
                    anchor_filters[kpi_data['Group 2 param 2']] = kpi_data['Group 2 value 2'].split(', ')
                else:
                    anchor_filters[kpi_data['Group 2 param 2']] = kpi_data['Group 2 value 2']
            if kpi_data['Group 2 param 3']:
                if ',' in kpi_data['Group 2 value 3']:
                    anchor_filters[kpi_data['Group 2 param 3']] = kpi_data['Group 2 value 3'].split(', ')
                else:
                    anchor_filters[kpi_data['Group 2 param 3']] = kpi_data['Group 2 value 3']
            direction_data = {
                'top': self._get_direction_for_relative_position(kpi_data['top']),
                'bottom': self._get_direction_for_relative_position(kpi_data['bottom']),
                'left': self._get_direction_for_relative_position(kpi_data['left']),
                'right': self._get_direction_for_relative_position(kpi_data['right'])}
            general_filters = {'template_name': scene_types}
            # if kpi_data['Sub category']:
            #     general_filters['sub_category'] = kpi_data['Sub category']
            # if kpi_data['SEGMENT']:
            #     general_filters['SEGMENT'] = kpi_data['SEGMENT'].split(', ')
            result = self.tools.calculate_relative_position(tested_filters, anchor_filters, direction_data,
                                                            **general_filters)
            if result:
                score = 1
            if return_result:
                self.related_kpi_results[kpi_name] = score
            else:
                self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)

    def calculate_average_shelf(self, kpi_set_fk, kpi_name, scene_types):
        template = self.average_shelf
        kpi_template = template.loc[template['KPI name'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        filters = {'template_name': scene_types,
                   kpi_template['entity group 1']: kpi_template['value group 1'].split(', ')}
        result = self.tools.calculate_average_shelf(**filters)
        score = result if result else 0
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)

    def calculate_average_shelf_new(self, kpi_set_fk, kpi_name, scene_types, category):
        template = self.average_shelf
        kpi_template = template.loc[(template['KPI name'] == kpi_name) & (template['category'] == category)]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        filters = {'template_name': scene_types, 'category': kpi_template['category']}
        new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
        category_space_kpi_name = CSPACE_KPI_NAME.format(category=category)
        result = 0
        if category_space_kpi_name in self.tools.average_shelf_values.keys():
            if self.tools.average_shelf_values[category_space_kpi_name].get('num_of_bays'):
                result = self.tools.average_shelf_values[category_space_kpi_name].get('num_of_shelves') / \
                         float(self.tools.average_shelf_values[category_space_kpi_name].get('num_of_bays'))
        score = result if result else 0
        self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3, result=score, score=score)

    def calculate_availability(self, kpi_set_fk, kpi_name, scene_type, return_result=False):
        if any(i in self.scif['template_name'].unique().tolist() for i in scene_type):
            kpi_template = self.availability_data.loc[self.availability_data['KPI name'] == kpi_name]
            if kpi_template.empty:
                return None
            result = 0
            kpi_template = kpi_template.iloc[0]

            filters_1 = {kpi_template['filter_1']: kpi_template['filter_1_value']}
            filters_2 = {kpi_template['filter_2']: kpi_template['filter_2_value']}
            if kpi_template['tow groups']:
                filters1 = {kpi_template['entity group 1']: [s for s in kpi_template['value group 1'].split(',')],
                            'template_name': scene_type}

                filters2 = {kpi_template['entity group 2']: [s for s in kpi_template['value group 2'].split(',')],
                            'template_name': scene_type}
                result1 = self.tools.calculate_availability(**filters1)
                result2 = self.tools.calculate_availability(**filters2)
                if result1 >= 1 and result2 >= 1:
                    result = 1
            else:
                filters = {kpi_template['entity group 1']: [s for s in kpi_template['value group 1'].split(',')],
                           'template_name': scene_type}
                if kpi_template['sub_category']:
                    filters['sub_category'] = kpi_template['sub_category']
                if kpi_template['PG SIZE']:
                    filters['PG SIZE'] = kpi_template['PG SIZE']
                if kpi_template['NATURALS']:
                    filters['NATURALS'] = kpi_template['NATURALS']
                result = self.tools.calculate_availability(**filters)
            score = 1 if result >= 1 else 0  # target is 1 facing
            if return_result:
                self.related_kpi_results[kpi_name] = score
            try:
                self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=result, score=score)
            except Exception as e:
                Log.info('KPI {} is not defined in the DB'.format(kpi_name))

    def calculate_related_kpi(self, kpi_set_fk, kpi_name, related_results):
        if self.related_kpi_results[kpi_name] > 0:
            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3,
                                    result=self.related_kpi_results[kpi_name], score=self.related_kpi_results[kpi_name])
        else:
            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=0, score=0)

    def calculate_anchor(self, kpi_set_fk, kpi_name, scene_type, category=None, list_type=False, return_result=False):
        if any(i in self.scif['template_name'].unique().tolist() for i in scene_type):
            if list_type:
                kpi_template = self.anchor_data.loc[self.anchor_data['kpi group'] == kpi_name]
            else:
                kpi_template = self.anchor_data.loc[self.anchor_data['KPI name'] == kpi_name]
            if kpi_template.empty:
                return None
            list_result = []
            score = 0
            values_to_check = []
            secondary_values_to_check = []
            if list_type:
                kpi_data = kpi_template.iloc[0]
                for s_type in scene_type:
                    if 'LL' in s_type:
                        position = 'left'
                        if 'reverse' in kpi_data['KPI name'] or 'Reverse' in kpi_data['KPI name']:
                            position = 'right'
                    else:
                        position = 'right'
                        if 'reverse' in kpi_data['KPI name'] or 'Reverse' in kpi_data['KPI name']:
                            position = 'left'
                    scif_scenes = self.scif.loc[self.scif['template_name'] == s_type]['scene_id'].unique().tolist()
                    filters = {'scene_id': scif_scenes}
                    if scif_scenes:
                        # if kpi_data['Audience']:
                        #     if ',' in kpi_data['Audience']:
                        #         filters['AUDIENCE'] = kpi_data['Audience'].split(', ')
                        #     else:
                        #         filters['AUDIENCE'] = kpi_data['Audience']
                        # if kpi_data['Brand']:
                        #     filters['brand_name'] = kpi_data['Brand'].split(', ')
                        # if kpi_data['Segment']:
                        #     filters['SEGMENT'] = kpi_template['Segment'].values[0]
                        # if kpi_data['SUPER CATEGORY']:
                        #     filters['SUPER CATEGORY'] = kpi_template['SUPER CATEGORY'].values[0]
                        # if kpi_data['NATURALS']:
                        #     filters['NATURALS'] = kpi_template['NATURALS'].values[0]
                        # if kpi_data['PRIVATE LABEL']:
                        #     filters['PRIVATE LABEL'] = kpi_template['PRIVATE LABEL'].values[0]
                        # if kpi_data['MANUFACTURER']:
                        #     filters['manufacturer_name'] = kpi_template['MANUFACTURER'].values[0]
                        if kpi_template['filter_1']:
                            values_to_check = \
                                self.all_products.loc[(self.all_products['category'] == kpi_template['category'])
                                                      & (self.all_products['product_type'] == 'SKU')][
                                    kpi_template['filter_1']].unique().tolist()
                        if kpi_template['filter_2']:
                            secondary_values_to_check = \
                                self.all_products.loc[(self.all_products['category'] == kpi_template['category'])
                                                      & (self.all_products['product_type'] == 'SKU')][
                                    kpi_template['filter_2']].unique().tolist()

                        for primary_filter in values_to_check:
                            filters[kpi_template['filter_1']] = primary_filter
                            if secondary_values_to_check:
                                for secondary_filter in secondary_values_to_check:
                                    if self.all_products[
                                                (self.all_products[kpi_template['filter_1']] == primary_filter) &
                                                (self.all_products[
                                                     kpi_template['filter_2']] == secondary_filter)].empty:
                                        continue
                                    filters[kpi_template['filter_2']] = secondary_filter
                                    results = self.tools.calculate_products_on_edge(list_result=list_type,
                                                                                    position=position,
                                                                                    category=category, **filters)
                                    if not results.empty:
                                        list_result.append(results[kpi_data['lead']].values.tolist())
                            else:
                                results = self.tools.calculate_products_on_edge(list_result=list_type,
                                                                                position=position,
                                                                                category=category, **filters)
                                if not results.empty:
                                    list_result.append(results[kpi_data['lead']].values.tolist())
                                    # results = self.tools.calculate_products_on_edge(list_result=list_type, position=position,
                                    #                                                 category=category, **filters)
                                    # if not results.empty:
                                    #     list_result.append(results[kpi_data['lead']].values.tolist())
                    if list_result:
                        list_result_new = [item for so in list_result for item in so]
                        # kpi_names = kpi_template.loc[kpi_template['kpi group'] == kpi_name]['KPI name'].unique().tolist()
                        score_dict = {}
                        new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                        for result in list(set(list_result_new)):
                            score_dict[result] = list_result_new.count(result)
                        i = 0
                        for result in score_dict.keys():
                            result = result.replace("'", "\\'")
                            # self.write_to_db_result(kpi_set_fk, kpi_name=kpi_names[i], level=self.LEVEL3, result=result,
                            #                         score=score_dict[result])
                            self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name + i, level=self.LEVEL3,
                                                    result=result,
                                                    score=score_dict[result])
                            i += 1
            else:
                filters = {}
                kpi_data = kpi_template.iloc[0]
                if kpi_data['Brand']:
                    filters['brand_name'] = kpi_template['Brand'].values[0]
                if kpi_data['Segment']:
                    filters['SEGMENT'] = kpi_template['Segment'].values[0]
                if kpi_data['SUPER CATEGORY']:
                    filters['SUPER CATEGORY'] = kpi_template['SUPER CATEGORY'].values[0]
                if kpi_data['NATURALS']:
                    filters['NATURALS'] = kpi_template['NATURALS'].values[0]
                if kpi_data['PRIVATE LABEL']:
                    filters['PRIVATE LABEL'] = kpi_template['PRIVATE LABEL'].values[0]
                if kpi_data['MANUFACTURER']:
                    filters['manufacturer_name'] = kpi_template['MANUFACTURER'].values[0]
                for s_type in scene_type:
                    if 'LL' in s_type:
                        position = 'left'
                        if 'reverse' in kpi_name:
                            position = 'right'
                    else:
                        position = 'right'
                        if 'reverse' in kpi_name:
                            position = 'left'
                    scif_scenes = self.scif.loc[self.scif['template_name'] == s_type]['scene_id'].unique().tolist()
                    filters['scene_id'] = scif_scenes
                    results = self.tools.calculate_products_on_edge(list_result=list_type, position=position,
                                                                    category=category, **filters)
                    if results[0] >= 1:
                        score = 1
                    if return_result:
                        self.related_kpi_results[kpi_name] = score
                    self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score,
                                            score=score)
                return

    @kpi_runtime()
    def calculate_anchor_new(self, kpi_set_fk, kpi_name, scene_type, category=None, list_type=False,
                             return_result=False,
                             filters=None):
        if any(i in self.scif['template_name'].unique().tolist() for i in scene_type):
            if list_type:
                kpi_template = self.anchor_data.loc[self.anchor_data['kpi group'] == kpi_name]
            else:
                kpi_template = self.anchor_data.loc[(self.anchor_data['KPI name'] == kpi_name) &
                                                    (self.anchor_data['category'] == category)]
            if kpi_template.empty:
                return None
            if kpi_template['list'].values[0] == 'Y':
                list_type = True
            list_result = []
            score = 0
            values_to_check = []
            secondary_values_to_check = []
            if list_type:
                kpi_data = kpi_template.iloc[0]
                for s_type in scene_type:
                    if 'LL' in s_type:
                        position = 'left'
                        if 'rev' in kpi_data['KPI name'] or 'Rev' in kpi_data['KPI name']:
                            position = 'right'
                    else:
                        position = 'right'
                        if 'rev' in kpi_data['KPI name'] or 'Rev' in kpi_data['KPI name']:
                            position = 'left'
                    scif_scenes = self.scif.loc[self.scif['template_name'] == s_type]['scene_id'].unique().tolist()
                    filters = {'scene_id': scif_scenes}
                    if scif_scenes:
                        # if kpi_data['Audience']:
                        #     if ',' in kpi_data['Audience']:
                        #         filters['AUDIENCE'] = kpi_data['Audience'].split(', ')
                        #     else:
                        #         filters['AUDIENCE'] = kpi_data['Audience']
                        # if kpi_data['Brand']:
                        #     filters['brand_name'] = kpi_data['Brand'].split(', ')
                        # if kpi_data['Segment']:
                        #     filters['SEGMENT'] = kpi_template['Segment'].values[0]
                        # if kpi_data['SUPER CATEGORY']:
                        #     filters['SUPER CATEGORY'] = kpi_template['SUPER CATEGORY'].values[0]
                        # if kpi_data['NATURALS']:
                        #     filters['NATURALS'] = kpi_template['NATURALS'].values[0]
                        # if kpi_data['PRIVATE LABEL']:
                        #     filters['PRIVATE LABEL'] = kpi_template['PRIVATE LABEL'].values[0]
                        # if kpi_data['MANUFACTURER']:
                        #     filters['manufacturer_name'] = kpi_template['MANUFACTURER'].values[0]
                        # if kpi_data['filter_1']:
                        #     values_to_check = \
                        #     self.all_products.loc[(self.all_products['category'] == kpi_data['category'])
                        #                             & (self.all_products['product_type'] == 'SKU')][
                        #         kpi_data['filter_1']].unique().tolist()
                        # if kpi_data['filter_2']:
                        #     secondary_values_to_check = \
                        #     self.all_products.loc[(self.all_products['category'] == kpi_data['category'])
                        #                             & (self.all_products['product_type'] == 'SKU')][
                        #         kpi_data['filter_2']].unique().tolist()

                        # for primary_filter in values_to_check:
                        #     filters[kpi_data['filter_1']] = primary_filter
                        #     if secondary_values_to_check:
                        #         for secondary_filter in secondary_values_to_check:
                        #             if self.all_products[
                        #                         (self.all_products[kpi_data['filter_1']] == primary_filter) &
                        #                         (self.all_products[
                        #                              kpi_data['filter_2']] == secondary_filter)].empty:
                        #                 continue
                        #             filters[kpi_data['filter_2']] = secondary_filter
                        #             results = self.tools.calculate_products_on_edge(list_result=list_type,
                        #                                                             position=position,
                        #                                                             category=category, **filters)
                        #             if not results.empty:
                        #                 list_result.append(results[kpi_data['lead']].values.tolist())
                        #     else:
                        #         results = self.tools.calculate_products_on_edge(list_result=list_type,
                        #                                                         position=position,
                        #                                                         category=category, **filters)
                        #         if not results.empty:
                        #             list_result.append(results[kpi_data['lead']].values.tolist())
                        if kpi_data['category'] in FABRICARE_CATEGORIES:
                            category_att = PG_CATEGORY
                            category = 'FABRICARE'
                            filters[category_att] = kpi_data['category']
                        results = self.tools.calculate_products_on_edge(list_result=list_type, position=position,
                                                                        category=category, **filters)
                        if not results.empty:
                            list_result.append(results[kpi_data['lead']].values.tolist())
                    if list_result:
                        list_result_new = [item for so in list_result for item in so]
                        # kpi_names = kpi_template.loc[kpi_template['kpi group'] == kpi_name]['KPI name'].unique().tolist()
                        score_dict = {}
                        filters['category'] = kpi_data['category']
                        del filters[PG_CATEGORY]
                        new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                        if kpi_data['secondary_lead']:
                            for result in list(set(list_result_new)):
                                relevant_sec_atts = self.all_products[self.all_products[kpi_data['lead']] == result][
                                    kpi_data['secondary_lead']].unique().tolist()
                                for sec_att in relevant_sec_atts:
                                    if sec_att in results[
                                        kpi_data['secondary_lead']].values.tolist() and not results.empty:
                                        score_dict[result + '/' + sec_att] = results.loc[
                                            (results[kpi_data['lead']] == result) & (results[kpi_data['secondary_lead']]
                                                                                     == sec_att)].count
                        for result in list(set(list_result_new)):
                            score_dict[result] = list_result_new.count(result)
                        i = 0
                        for result in score_dict.keys():
                            i += 1
                            result = result.replace("'", "\\'")
                            # self.write_to_db_result(kpi_set_fk, kpi_name=kpi_names[i], level=self.LEVEL3, result=result,
                            #                         score=score_dict[result])
                            self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name + ' ' + str(i), level=self.LEVEL3,
                                                    result=result,
                                                    score=score_dict[result])
            else:
                # filters = {}
                # kpi_data = kpi_template.iloc[0]
                # if kpi_data['Brand']:
                #     filters['brand_name'] = kpi_template['Brand'].values[0]
                # if kpi_data['Segment']:
                #     filters['SEGMENT'] = kpi_template['Segment'].values[0]
                # if kpi_data['SUPER CATEGORY']:
                #     filters['SUPER CATEGORY'] = kpi_template['SUPER CATEGORY'].values[0]
                # if kpi_data['NATURALS']:
                #     filters['NATURALS'] = kpi_template['NATURALS'].values[0]
                # if kpi_data['PRIVATE LABEL']:
                #     filters['PRIVATE LABEL'] = kpi_template['PRIVATE LABEL'].values[0]
                # if kpi_data['MANUFACTURER']:
                #     filters['manufacturer_name'] = kpi_template['MANUFACTURER'].values[0]
                if filters is None:
                    filters = {}
                for s_type in scene_type:
                    if 'LL' in s_type:
                        position = 'left'
                        if 'rev' in kpi_name:
                            position = 'right'
                    else:
                        position = 'right'
                        if 'rev' in kpi_name:
                            position = 'left'
                    scif_scenes = self.scif.loc[self.scif['template_name'] == s_type]['scene_id'].unique().tolist()
                    filters['scene_id'] = scif_scenes
                    results = self.tools.calculate_products_on_edge(list_result=list_type, position=position,
                                                                    category=category, **filters)
                    if results[0] >= 1:
                        score = 1
                    if return_result:
                        self.related_kpi_results[kpi_name] = score
                    else:
                        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score,
                                                score=score)
                return

    def calculate_count_of(self, kpi_set_fk, kpi_name, scene_type):
        kpi_template = self.count_of_data.loc[self.count_of_data['KPI name'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        if kpi_template.empty:
            return None
        scif_scenes = self.scif.loc[self.scif['template_name'].isin(scene_type)]
        scenes = scif_scenes['scene_id'].unique().tolist()
        if scenes is not None:
            # filters = {'scene_id': scenes, 'Sub Brand': kpi_template['Sub Brand'],
            #            'sub_category': kpi_template['Sub category'],
            #            'manufacturer_name': kpi_template['manufacturer name']}
            filters = {'scene_id': scenes}
            if kpi_template['Sub Brand']:
                filters['Sub Brand'] = kpi_template['Sub Brand']
            if kpi_template['Sub category']:
                filters['sub_category'] = kpi_template['Sub category']
            if kpi_template['category']:
                filters['category'] = kpi_template['category']
            if kpi_template['manufacturer name']:
                filters['manufacturer_name'] = kpi_template['manufacturer name']
            if kpi_template['SEGMENT']:
                filters['SEGMENT'] = kpi_template['SEGMENT']
            if kpi_template['NATURALS']:
                filters['NATURALS'] = kpi_template['NATURALS']
            if kpi_template['Brand']:
                filters['brand_name'] = kpi_template['Brand']
            if kpi_template['count'] == 'facings':
                result = self.tools.calculate_availability(**filters)
            else:
                result = self.tools.calculate_assortment(assortment_entity=kpi_template['count'], **filters)
            if kpi_template['score'] == 'number':
                self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=result,
                                        score=int(result))

    @kpi_runtime()
    def calculate_count_of_new(self, kpi_set_fk, kpi_name, scene_type, category):
        kpi_template = self.count_of_data.loc[(self.count_of_data['KPI name'] == kpi_name) &
                                              (self.count_of_data['category'] == category)]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        if kpi_template.empty:
            return None
        scif_scenes = self.scif.loc[self.scif['template_name'].isin(scene_type)]
        scenes = scif_scenes['scene_id'].unique().tolist()
        if scenes is not None:
            # filters = {'scene_id': scenes, 'Sub Brand': kpi_template['Sub Brand'],
            #            'sub_category': kpi_template['Sub category'],
            #            'manufacturer_name': kpi_template['manufacturer name']}
            filters = {'scene_id': scenes, 'category': kpi_template['category']}
            if kpi_template['category'] in FABRICARE_CATEGORIES:
                category_att = PG_CATEGORY
            else:
                category_att = 'category'
            values_to_check = []
            secondary_values_to_check = []
            if kpi_template['filter_1']:
                values_to_check = self.all_products.loc[(self.all_products[category_att] == kpi_template['category'])
                                                        & (self.all_products['product_type'] == 'SKU')][
                    kpi_template['filter_1']].unique().tolist()
            # if kpi_template['filter_2']:
            #     secondary_values_to_check = \
            #     self.all_products.loc[(self.all_products['category'] == kpi_template['category'])
            #                           & (self.all_products['product_type'] == 'SKU')][
            #         kpi_template['filter_2']].unique().tolist()

            for primary_filter in values_to_check:
                filters[kpi_template['filter_1']] = primary_filter
                if kpi_template['filter_2']:
                    secondary_values_to_check = \
                        self.all_products.loc[(self.all_products[category_att] == kpi_template['category']) &
                                              (self.all_products[kpi_template['filter_1']] == primary_filter)
                                              & (self.all_products['product_type'] == 'SKU')][
                            kpi_template['filter_2']].unique().tolist()
                if secondary_values_to_check:
                    for secondary_filter in secondary_values_to_check:
                        if secondary_filter is None:
                            continue
                        filters[kpi_template['filter_2']] = secondary_filter
                        filtered_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
                        filtered_df2 = filtered_df[self.tools.get_filter_condition(filtered_df, **filters)]
                        new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                        if 'SEGMENT' in filters.keys():
                            filters['PRIVATE_LABEL'] = 'N'
                        if filtered_df2.empty:
                            result = 0
                        else:
                            if kpi_template['category'] in FABRICARE_CATEGORIES:
                                filters[PG_CATEGORY] = kpi_template['category']
                                if 'category' in filters.keys():
                                    del filters['category']
                            if kpi_template['count'] == 'facings':
                                result = self.tools.calculate_availability(**filters)
                            else:
                                result = self.tools.calculate_assortment(assortment_entity=kpi_template['count'],
                                                                         **filters)
                        filters['category'] = kpi_template['category']
                        if kpi_template['score'] == 'number':
                            self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3, result=result,
                                                    score=int(result))
                else:
                    new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                    if kpi_template['category'] in FABRICARE_CATEGORIES:
                        filters[PG_CATEGORY] = kpi_template['category']
                        if 'category' in filters.keys():
                            del filters['category']
                        if 'SEGMENT' in filters.keys():
                            filters['PRIVATE_LABEL'] = 'N'
                    filtered_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
                    if filtered_df.empty:
                        result = 0
                    else:
                        if kpi_template['count'] == 'facings':
                            result = self.tools.calculate_availability(**filters)
                        else:
                            result = self.tools.calculate_assortment(assortment_entity=kpi_template['count'], **filters)
                    filters['category'] = kpi_template['category']
                    if kpi_template['score'] == 'number':
                        self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3, result=result,
                                                score=int(result))

    def calculate_posm_availability(self, kpi_set_fk, kpi_name, scene_type):
        kpi_template = self.posm_data.loc[self.posm_data['KPI name'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        if kpi_template.empty:
            return None
        scif_scenes = self.scif.loc[self.scif['template_name'].isin(scene_type)]
        scenes = scif_scenes['scene_id'].unique().tolist()
        if scenes is not None:
            filters = {'scene_id': scenes, 'category': kpi_template['category']}
            values_to_check = []
            secondary_values_to_check = []
            if kpi_template['filter_1']:
                values_to_check = [kpi_template['filter_1_value']]
            if kpi_template['filter_2']:
                secondary_values_to_check = [kpi_template['filter_2_value']]
            for primary_filter in values_to_check:
                filters[kpi_template['filter_1']] = primary_filter
                if secondary_values_to_check:
                    for secondary_filter in secondary_values_to_check:
                        if self.all_products[(self.all_products[kpi_template['filter_1']] == primary_filter) &
                                (self.all_products[kpi_template['filter_2']] == secondary_filter)].empty:
                            continue
                        filters[kpi_template['filter_2']] = secondary_filter
                        if kpi_template['count'] == 'facings':
                            result = self.tools.calculate_availability(**filters)
                        else:
                            result = self.tools.calculate_assortment(**filters)
                        final_result = 1 if result else 0
                        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=final_result,
                                                score=int(result))
                else:
                    if kpi_template['count'] == 'facings':
                        result = self.tools.calculate_availability(**filters)
                    else:
                        result = self.tools.calculate_assortment(assortment_entity=kpi_template['count'], **filters)
                    final_result = 1 if result else 0
                    self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=final_result,
                                            score=int(result))

    def calculate_orchestrated(self, kpi_set_fk, kpi_name, scene_types):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        kpi_data = self.orchestrated_data.loc[self.orchestrated_data['KPI name'] == kpi_name]
        score = 0
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        filters = (kpi_data['filter attribute'].encode(), [s for s in kpi_data['attribute'].split(', ')])
        filter_attributes_index_dict = {}
        i = 0
        for attributes in filters[1]:
            filter_attributes_index_dict[attributes] = i
            i += 1
        direction_data = [s for s in kpi_data['directions'].split(', ')]
        for direction in direction_data:
            if kpi_data['sub_brand']:
                general_filters = {'template_name': scene_types, 'Sub Brand': kpi_data['sub_brand'],
                                   'SEGMENT': kpi_data['Segment']}
            else:
                general_filters = {'template_name': scene_types,
                                   'SEGMENT': kpi_data['Segment']}
            if kpi_data['P&G BRAND']:
                general_filters['P&G BRAND'] = kpi_data['P&G BRAND']
            if kpi_data['SUPER CATEGORY']:
                general_filters['SUPER CATEGORY'] = kpi_data['SUPER CATEGORY']
            try:
                if 'vertical' in kpi_data['KPI name'] or 'Vertical' in kpi_data['KPI name']:
                    # result = False
                    result = self.tools.calculate_vertical_product_sequence_per_bay(sequence_filters=filters,
                                                                                    direction=direction,
                                                                                    filter_attributes_index_dict=filter_attributes_index_dict,
                                                                                    **general_filters)
                else:
                    result = self.tools.calculate_product_sequence_per_shelf(sequence_filters=filters,
                                                                             direction=direction,
                                                                             filter_attributes_index_dict=filter_attributes_index_dict,
                                                                             **general_filters)
            except Exception as e:
                Log.info('Orchestrated calculation failed due to {}'.format(e))
                result = False
            score += 1 if result else 0
        scores = 1 if score > 0 else 0
        try:
            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=scores, score=scores)
        except Exception as e:
            Log.info('KPI {} does not exist in the DB'.format(kpi_name))

    def calculate_orchestrated_new(self, kpi_set_fk, kpi_name, scene_types):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        kpi_data = self.orchestrated_data.loc[self.orchestrated_data['KPI name'] == kpi_name]
        score = 0
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        filters = (kpi_data['filter attribute'].encode(), [s for s in kpi_data['attribute'].split(', ')])
        filter_attributes_index_dict = {}
        i = 0
        for attributes in filters[1]:
            filter_attributes_index_dict[attributes] = i
            i += 1
        direction_data = [s for s in kpi_data['directions'].split(', ')]
        general_filters = {'template_name': scene_types}
        for direction in direction_data:
            try:
                if kpi_data['vertical'] == 'Y':
                    # result = False
                    result = self.tools.calculate_vertical_product_sequence_per_bay(sequence_filters=filters,
                                                                                    direction=direction,
                                                                                    filter_attributes_index_dict=filter_attributes_index_dict,
                                                                                    **general_filters)
                else:
                    result = self.tools.calculate_product_sequence_per_shelf(sequence_filters=filters,
                                                                             direction=direction,
                                                                             filter_attributes_index_dict=filter_attributes_index_dict,
                                                                             **general_filters)
            except Exception as e:
                Log.info('Orchestrated calculation failed due to {}'.format(e))
                result = False
            score += 1 if result else 0

        if kpi_data['custom orchestration'] == 'Y':
            filters_for_anchor = {'PRICE SEGMENT': 'ECONOMY'}
            self.calculate_anchor_new(kpi_set_fk, kpi_name + ' anchor', scene_types, filters=filters_for_anchor)
            filters_for_anchor_reverse = {'PRICE SEGMENT': 'PREMIUM'}
            self.calculate_anchor_new(kpi_set_fk, kpi_name + ' reverse', scene_types,
                                      filters=filters_for_anchor_reverse)
            tested_filters = {'PRICE SEGMENT': 'SUPER PREMIUM'}
            anchor_filters = {'PRICE SEGMENT': ['PREMIUM', 'ECONOMY']}
            direction_data = {'top', 'bottom'}
            relative_pos_result = self.tools.calculate_relative_position(tested_filters, anchor_filters, direction_data,
                                                                         **general_filters)
            if relative_pos_result and self.related_kpi_results[kpi_name + ' anchor'] \
                    and self.related_kpi_results[kpi_name + ' reverse']:
                score = 1
            else:
                score = 0
        scores = 1 if score > 0 else 0
        try:
            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=scores, score=scores)
        except Exception as e:
            Log.info('KPI {} does not exist in the DB'.format(kpi_name))

    def calculate_adjacency(self, kpi_set_fk, kpi_name, scene_types, list_type=True):
        if list_type:
            kpi_data = self.adjacency_data.loc[self.adjacency_data['kpi group'] == kpi_name]
        else:
            kpi_data = self.adjacency_data.loc[self.adjacency_data['KPI name'] == kpi_name]
        if kpi_data.empty:
            return None
        # kpi_data = kpi_data.iloc[0]
        general_filters = {'template_name': scene_types}
        direction_data = {'top': (1, 1), 'bottom': (1, 1), 'left': (1, 1), 'right': (1, 1)}
        tested_filters = {'SEGMENT': kpi_data['SEGMENT'].unique()[0].split(', ')}
        kpi_data = kpi_data.iloc[0]
        for param in ADJACENCY_PARAMS:
            if kpi_data[param]:
                if ',' in kpi_data[param]:
                    tested_filters[param] = kpi_data[param].split(', ')
                else:
                    tested_filters[param] = kpi_data[param]
        results = self.tools.calculate_adjacency_relativeness(tested_filters, direction_data,
                                                              **general_filters)
        if results:
            if list_type:
                kpi_data = self.adjacency_data.loc[self.adjacency_data['kpi group'] == kpi_name]
            kpi_names = kpi_data.loc[kpi_data['kpi group'] == kpi_name]['KPI name'].unique().tolist()
            score_dict = {}
            for result in list(set(results)):
                score_dict[result] = results.count(result)
            i = 0
            score_dict_list = sorted(score_dict.items(), key=lambda kv: kv[1], reverse=True)
            for result in score_dict_list:
                self.write_to_db_result(kpi_set_fk, kpi_name=kpi_names[i], level=self.LEVEL3, result=result[0],
                                        score=result[1])
                i += 1

    @kpi_runtime()
    def calculate_adjacency_new(self, kpi_set_fk, kpi_name, scene_types, category, list_type=True):

        kpi_data = self.adjacency_data.loc[(self.adjacency_data['KPI name'] == kpi_name) &
                                           (self.adjacency_data['category'] == category)]
        if kpi_data.empty:
            return None
        # kpi_data = kpi_data.iloc[0]
        general_filters = {'template_name': scene_types}
        direction_data = {'top': (1, 1), 'bottom': (1, 1), 'left': (1, 1), 'right': (1, 1)}
        kpi_data = kpi_data.iloc[0]
        values_to_check = []
        secondary_values_to_check = []
        tested_filters = {'category': kpi_data['category']}
        if kpi_data['filter_1']:
            values_to_check = self.all_products.loc[(self.all_products['category'] == kpi_data['category'])
                                                    & (self.all_products['product_type'] == 'SKU')][
                kpi_data['filter_1']].unique().tolist()
        if kpi_data['filter_2']:
            secondary_values_to_check = \
                self.all_products.loc[(self.all_products['category'] == kpi_data['category'])
                                      & (self.all_products['product_type'] == 'SKU')][
                    kpi_data['filter_2']].unique().tolist()

        for primary_filter in values_to_check:
            tested_filters[kpi_data['filter_1']] = primary_filter
            if secondary_values_to_check:
                for secondary_filter in secondary_values_to_check:
                    if self.all_products[(self.all_products[kpi_data['filter_1']] == primary_filter) &
                            (self.all_products[kpi_data['filter_2']] == secondary_filter)].empty:
                        continue
                    tested_filters[kpi_data['filter_2']] = secondary_filter
                    new_kpi_name = self.kpi_name_builder(kpi_name, **tested_filters)
                    results = self.tools.calculate_adjacency_relativeness(tested_filters, direction_data,
                                                                          **general_filters)
                    if results:
                        score_dict = {}
                        for result in list(set(results)):
                            score_dict[result] = results.count(result)
                        i = 0
                        score_dict_list = sorted(score_dict.items(), key=lambda kv: kv[1], reverse=True)
                        for result in score_dict_list:
                            new_result = result[0].replace("'", "\\'")
                            if "'" in new_result:
                                continue
                            self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name + ' ' + str(i), level=self.LEVEL3,
                                                    result=new_result,
                                                    score=result[1])
                            i += 1
            else:
                new_kpi_name = self.kpi_name_builder(kpi_name, **tested_filters)
                results = self.tools.calculate_adjacency_relativeness(tested_filters, direction_data,
                                                                      **general_filters)
                if results:
                    score_dict = {}
                    for result in list(set(results)):
                        score_dict[result] = results.count(result)
                    i = 0
                    score_dict_list = sorted(score_dict.items(), key=lambda kv: kv[1], reverse=True)
                    for result in score_dict_list:
                        if type(result) in [float, int]:
                            continue
                        new_result = result[0].replace("'", "\'")
                        if "'" in new_result:
                            continue
                        self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name + ' ' + str(i), level=self.LEVEL3,
                                                result=new_result,
                                                score=result[1])
                        i += 1

    @kpi_runtime()
    def calculate_block_together(self, kpi_set_fk, kpi_name, scene_type, return_result=False):
        if set(self.scif['template_name'].unique().tolist()) & set(scene_type):
            block_template = self.block_data.loc[self.block_data['kpi group'] == kpi_name]
            kpi_template = block_template.iloc[0]
            save_list = []
            vertical, horizontal, orphan, group, general, block_of_blocks = False, False, False, False, False, False
            block_products = None
            group_products = None
            block_products1 = None
            block_products2 = None
            if kpi_template.empty:
                return
            if kpi_template['Regular Block']:
                general = True
                save_list.append('regular block')
            if kpi_template['Vertical Block']:
                vertical = True
                save_list.append('Vertical Block')
            if kpi_template['horizontally block']:
                horizontal = True
                save_list.append('horizontally block')
            if kpi_template['Orphan products']:
                orphan = True
                save_list.append('Orphan products')
            if kpi_template['group in block']:
                group = True
                save_list.append('group in block')
                block_products = {'template_name': scene_type, kpi_template['attribute']: kpi_template['block']}
                group_products = {'template_name': scene_type, kpi_template['attribute']: kpi_template['group']}
            if kpi_template['block in block']:
                block_of_blocks = True
                save_list.append('block in block')
                # block_products1 = {'template_name': scene_type, kpi_template['attribute']: kpi_template['value']}
                # block_products2 = {'template_name': scene_type, kpi_template['attribute1']: kpi_template['value1']}
                block_products1 = {kpi_template['attribute']: kpi_template['value']}
                block_products2 = {kpi_template['attribute1']: kpi_template['value1']}
            filters = {}
            segment = False
            for param in BLOCK_PARAMS:
                if kpi_template[param]:
                    if param == 'SEGMENT':
                        segment = True
                    if param == 'sub_category':
                        if segment:
                            continue
                    if ',' in kpi_template[param]:
                        filters[param] = kpi_template[param].split(', ')
                    else:
                        filters[param] = kpi_template[param]

            filters['template_name'] = scene_type
            include_empty = False
            res = self.tools.calculate_block_together(include_empty=include_empty, minimum_block_ratio=0.75,
                                                      vertical=vertical,
                                                      horizontal=horizontal, orphan=orphan, group=group,
                                                      block_products=block_products,
                                                      group_products=group_products, block_of_blocks=block_of_blocks,
                                                      block_products1=block_products1,
                                                      block_products2=block_products2, **filters)
            if type(res) == str and res == 'no_products':
                return
            if res:
                if not general:
                    res.pop('regular block')
                for kpi in res.keys():
                    name = block_template.loc[block_template['kpi type'] == kpi]['KPI name'].values[0]
                    try:
                        self.write_to_db_result(kpi_set_fk, kpi_name=name, level=self.LEVEL3,
                                                result=1 if res[kpi] else 0,
                                                score=1 if res[kpi] else 0)
                    except IndexError as e:
                        Log.info('Saving KPI {} failed due to {}'.format(kpi_name, e))

            else:
                for kpi in save_list:
                    name = block_template.loc[block_template['kpi type'] == kpi]['KPI name'].values[0]
                    try:
                        self.write_to_db_result(kpi_set_fk, kpi_name=name, level=self.LEVEL3, result=0, score=0)
                    except IndexError as e:
                        Log.info('Saving KPI {} failed due to {}'.format(kpi_name, e))
            if return_result:
                self.related_kpi_results[kpi_name] = res

    @kpi_runtime()
    def calculate_block_together_new(self, kpi_set_fk, kpi_name, scene_type, category):
        if set(self.scif['template_name'].unique().tolist()) & set(scene_type):
            block_template = self.block_data.loc[(self.block_data['KPI name'] == kpi_name) &
                                                 (self.block_data['category'] == category)]
            kpi_template = block_template.iloc[0]
            save_list = []
            vertical, horizontal, orphan, group, general, block_of_blocks = False, False, False, False, False, False
            block_products = None
            group_products = None
            block_products1 = None
            block_products2 = None
            if kpi_template.empty:
                return
            if kpi_template['kpi type'] == 'Regular Block':
                general = True
                save_list.append('regular block')
            if kpi_template['kpi type'] == 'hor_vs_vertical':
                vertical = horizontal = True
            if kpi_template['kpi type'] == 'block in block':
                block_of_blocks = True
                save_list.append('block in block')
                block_products1 = {kpi_template['attribute']: kpi_template['value']}
                block_products2 = {kpi_template['attribute1']: kpi_template['value1']}
            include_empty = False
            values_to_check = []
            secondary_values_to_check = []
            filters = {'template_name': scene_type, 'category': kpi_template['category']}
            if kpi_template['category'] in FABRICARE_CATEGORIES:
                category_att = PG_CATEGORY
            else:
                category_att = 'category'
            if kpi_template['filter_1']:
                values_to_check = self.all_products.loc[(self.all_products[category_att] == kpi_template['category'])
                                                        & (self.all_products['product_type'] == 'SKU')][
                    kpi_template['filter_1']].unique().tolist()
            # if kpi_template['filter_2']:
            #     if kpi_template['filter_2_value']:
            #         secondary_values_to_check = [kpi_template['filter_2_value']]
            #     else:
            #         secondary_values_to_check = \
            #         self.all_products.loc[(self.all_products['category'] == kpi_template['category'])
            #                                         & (self.all_products['product_type'] == 'SKU')][
            #             kpi_template['filter_2']].unique().tolist()

            for primary_filter in values_to_check:
                filters[kpi_template['filter_1']] = primary_filter
                if kpi_template['filter_2']:
                    if kpi_template['filter_2_value']:
                        secondary_values_to_check = [kpi_template['filter_2_value']]
                    else:
                        secondary_values_to_check = \
                            self.all_products.loc[(self.all_products[category_att] == kpi_template['category']) &
                                                  (self.all_products[kpi_template['filter_1']] == primary_filter)
                                                  & (self.all_products['product_type'] == 'SKU')][
                                kpi_template['filter_2']].unique().tolist()
                if secondary_values_to_check:
                    for secondary_filter in secondary_values_to_check:
                        if self.all_products[(self.all_products[kpi_template['filter_1']] == primary_filter) &
                                (self.all_products[kpi_template['filter_2']] == secondary_filter)].empty:
                            continue
                        filters[kpi_template['filter_2']] = secondary_filter
                        if not block_of_blocks:
                            if kpi_template['filter_2'] == 'PRIVATE_LABEL':
                                del filters['PRIVATE_LABEL']
                            new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                            filters[kpi_template['filter_2']] = secondary_filter
                        else:
                            new_kpi_name = kpi_name
                        if kpi_template['category'] in FABRICARE_CATEGORIES:
                            filters[PG_CATEGORY] = kpi_template['category']
                            if 'category' in filters.keys():
                                del filters['category']
                        res = self.tools.calculate_block_together_new(include_empty=include_empty,
                                                                      minimum_block_ratio=0.75,
                                                                      vertical=vertical,
                                                                      horizontal=horizontal, orphan=orphan, group=group,
                                                                      block_products=block_products,
                                                                      group_products=group_products,
                                                                      block_of_blocks=block_of_blocks,
                                                                      block_products1=block_products1,
                                                                      block_products2=block_products2, **filters)
                        filters['category'] = kpi_template['category']
                        if type(res) == str and res == 'no_products':
                            return
                        self.block_results[new_kpi_name] = res['regular block'] if res else 0
                        if res:
                            result = 1 if res['regular block'] else 0
                            score = 1 if res['regular block'] else 0
                            if kpi_template['kpi type'] == 'hor_vs_vertical':
                                result = 'VERTICAL' if res['vertical'] else 'HORIZONTAL'
                                self.hor_ver_results[kpi_name] = result
                            if kpi_template['kpi type'] == 'block in block':
                                result = 1 if res['block_of_blocks'] else 0
                            try:
                                self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3,
                                                        result=result,
                                                        score=score)
                            except IndexError as e:
                                Log.info('Saving KPI {} failed due to {}'.format(kpi_name, e))

                        else:
                            try:
                                self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3,
                                                        result=0, score=0)
                            except IndexError as e:
                                Log.info('Saving KPI {} failed due to {}'.format(kpi_name, e))
                else:
                    if kpi_template['category'] in FABRICARE_CATEGORIES:
                        filters[PG_CATEGORY] = kpi_template['category']
                        if 'category' in filters.keys():
                            del filters['category']
                    res = self.tools.calculate_block_together_new(include_empty=include_empty, minimum_block_ratio=0.75,
                                                                  vertical=vertical,
                                                                  horizontal=horizontal, orphan=orphan, group=group,
                                                                  block_products=block_products,
                                                                  group_products=group_products,
                                                                  block_of_blocks=block_of_blocks,
                                                                  block_products1=block_products1,
                                                                  block_products2=block_products2, **filters)
                    filters['category'] = kpi_template['category']
                    if type(res) == str and res == 'no_products':
                        return
                    if not block_of_blocks:
                        new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                    else:
                        new_kpi_name = kpi_name
                    self.block_results[new_kpi_name] = res['regular block'] if res else 0
                    if res:
                        result = 1 if res['regular block'] else 0
                        score = 1 if res['regular block'] else 0
                        if kpi_template['kpi type'] == 'hor_vs_vertical':
                            result = 'VERTICAL' if res['vertical'] else 'HORIZONTAL'
                            self.hor_ver_results[kpi_name] = result
                        if kpi_template['kpi type'] == 'block in block':
                            result = 1 if res['block_of_blocks'] else 0
                        try:
                            self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3,
                                                    result=result,
                                                    score=score)
                        except IndexError as e:
                            Log.info('Saving KPI {} failed due to {}'.format(kpi_name, e))

                    else:
                        try:
                            self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3,
                                                    result=0, score=0)
                        except IndexError as e:
                            Log.info('Saving KPI {} failed due to {}'.format(kpi_name, e))
            if not values_to_check:
                res = self.tools.calculate_block_together_new(include_empty=include_empty, minimum_block_ratio=0.75,
                                                              vertical=vertical,
                                                              horizontal=horizontal, orphan=orphan, group=group,
                                                              block_products=block_products,
                                                              group_products=group_products,
                                                              block_of_blocks=block_of_blocks,
                                                              block_products1=block_products1,
                                                              block_products2=block_products2, **filters)
                if type(res) == str and res == 'no_products':
                    return
                if not block_of_blocks:
                    new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                else:
                    new_kpi_name = kpi_name
                self.block_results[new_kpi_name] = res['regular block'] if res else 0
                if res:
                    result = 1 if res['regular block'] else 0
                    score = 1 if res['regular block'] else 0
                    if kpi_template['kpi type'] == 'hor_vs_vertical':
                        result = 'VERTICAL' if res['vertical'] else 'HORIZONTAL'
                    if kpi_template['kpi type'] == 'block in block':
                        result = 1 if res['block_of_blocks'] else 0
                    try:
                        self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3,
                                                result=result,
                                                score=score)
                    except IndexError as e:
                        Log.info('Saving KPI {} failed due to {}'.format(kpi_name, e))

                else:
                    try:
                        self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3,
                                                result=0, score=0)
                    except IndexError as e:
                        Log.info('Saving KPI {} failed due to {}'.format(kpi_name, e))

    def calculate_checkerboarded(self, kpi_set_fk, kpi_name, scene_type, list_type=None):
        if set(self.scif['template_name'].unique().tolist()) & set(scene_type):
            if list_type:
                checkerboarded_template = self.checkerboarded_template.loc[
                    self.checkerboarded_template['checkerboarded_group'] == kpi_name]
            else:
                checkerboarded_template = self.checkerboarded_template.loc[
                    self.checkerboarded_template['KPI name'] == kpi_name]
            kpi_template = checkerboarded_template.iloc[0]
            if kpi_template.empty:
                return
            filters = {}
            if kpi_template['sub_category']:
                filters['sub_category'] = kpi_template['sub_category']
            if kpi_template['SEGMENT']:
                filters['SEGMENT'] = kpi_template['SEGMENT']
            if kpi_template['PRIVATE_LABEL']:
                filters['PRIVATE_LABEL'] = kpi_template['PRIVATE_LABEL']
            if kpi_template['brand_name']:
                filters['brand_name'] = kpi_template['brand_name']
            if kpi_template['category']:
                filters['category'] = kpi_template['category']

            filters['template_name'] = scene_type
            include_empty = False
            try:
                res = self.tools.calculate_block_together(include_empty=include_empty, minimum_block_ratio=0.75,
                                                          include_private_label=True, **filters)
            except Exception as e:
                res = False
            if type(res) == str and res == 'no_products':
                return
            if res:
                if kpi_template['brand_list']:
                    kpi_names = checkerboarded_template['KPI name'].unique().tolist()
                    i = 0
                    for result in res['brand_list']:
                        result = result.replace("'", "\\'")
                        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_names[i], level=self.LEVEL3, result=result,
                                                score=1)
                        i += 1
                else:
                    res.pop('regular block')
                    for kpi in res.keys():
                        try:
                            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3,
                                                    result=1 if res[kpi] else 0,
                                                    score=1 if res[kpi] else 0)
                        except IndexError as e:
                            Log.info('Saving KPI {} failed due to {}'.format(kpi_name, e))

            else:
                try:
                    if list_type:
                        kpi_name = kpi_template['checkerboarded_group']
                    self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=0, score=0)
                except IndexError as e:
                    Log.info('Saving KPI {} failed due to {}'.format(kpi_name, e))

    @kpi_runtime()
    def calculate_checkerboarded_new(self, kpi_set_fk, kpi_name, scene_type, category, list_type=None):
        if set(self.scif['template_name'].unique().tolist()) & set(scene_type):
            if list_type:
                checkerboarded_template = self.checkerboarded_template.loc[
                    self.checkerboarded_template['checkerboarded_group'] == kpi_name]
            else:
                checkerboarded_template = self.checkerboarded_template.loc[
                    (self.checkerboarded_template['KPI name'] == kpi_name) &
                    (self.checkerboarded_template['category'] == category)]
            kpi_template = checkerboarded_template.iloc[0]
            if kpi_template.empty:
                return
            values_to_check = []
            secondary_values_to_check = []
            filters = {'template_name': scene_type, 'category': kpi_template['category']}
            if kpi_template['category'] in FABRICARE_CATEGORIES:
                category_att = PG_CATEGORY
            else:
                category_att = 'category'
            if kpi_template['filter_1']:
                values_to_check = self.all_products.loc[(self.all_products[category_att] == kpi_template['category'])
                                                        & (self.all_products['product_type'] == 'SKU')][
                    kpi_template['filter_1']].unique().tolist()
            if kpi_template['filter_2']:
                secondary_values_to_check = \
                    self.all_products.loc[(self.all_products[category_att] == kpi_template['category'])
                                          & (self.all_products['product_type'] == 'SKU')][
                        kpi_template['filter_2']].unique().tolist()

            for primary_filter in values_to_check:
                filters[kpi_template['filter_1']] = primary_filter
                if secondary_values_to_check:
                    for secondary_filter in secondary_values_to_check:
                        if self.all_products[(self.all_products[kpi_template['filter_1']] == primary_filter) &
                                (self.all_products[kpi_template['filter_2']] == secondary_filter)].empty:
                            continue
                        filters[kpi_template['filter_2']] = secondary_filter
                        new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                        if kpi_template['category'] in FABRICARE_CATEGORIES:
                            filters[PG_CATEGORY] = kpi_template['category']
                            if 'category' in filters.keys():
                                del filters['category']
                        self.checkerboarded_writer(kpi_set_fk, kpi_template, new_kpi_name, filters)
                        filters['category'] = kpi_template['category']
                else:
                    new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                    if kpi_template['category'] in FABRICARE_CATEGORIES:
                        filters[PG_CATEGORY] = kpi_template['category']
                        if 'category' in filters.keys():
                            del filters['category']
                    self.checkerboarded_writer(kpi_set_fk, kpi_template, new_kpi_name, filters)

    def checkerboarded_writer(self, kpi_set_fk, kpi_template, kpi_name, filters):
        include_empty = False
        try:
            res = self.tools.calculate_block_together_new(include_empty=include_empty, minimum_block_ratio=0.75,
                                                          include_private_label=True, checkerboard=True, **filters)
        except Exception as e:
            res = False
        if type(res) == str and res == 'no_products':
            return
        if res:
            if kpi_template['brand_list']:
                i = 0
                for result in res['brand_list']:
                    result = result.replace("'", "\\'")
                    i += 1
                    self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name + ' ' + str(i), level=self.LEVEL3,
                                            result=result,
                                            score=1)
            else:
                res.pop('regular block')
                for kpi in res.keys():
                    try:
                        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3,
                                                result=1 if res[kpi] else 0,
                                                score=1 if res[kpi] else 0)
                    except IndexError as e:
                        Log.info('Saving KPI {} failed due to {}'.format(kpi_name, e))

        else:
            try:
                if kpi_template['brand_list']:
                    self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=0, score=0)
            except IndexError as e:
                Log.info('Saving KPI {} failed due to {}'.format(kpi_name, e))

    @kpi_runtime()
    def calculate_linear_feet(self, kpi_set_fk, kpi_name, scene_types, return_result=False):
        template = self.linear_feet_data.loc[self.linear_feet_data['KPI name'] == kpi_name]
        kpi_template = template.loc[template['KPI name'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        filters = {'template_name': scene_types, 'category': kpi_template['category']}

        # TODO repclace redundent code with somthing dynamic
        if kpi_template['Sub category']:
            filters['sub_category'] = kpi_template['Sub category']
        if kpi_template['manufacturer name']:
            filters['manufacturer_name'] = kpi_template['manufacturer name']
        if kpi_template['Sub Brand']:
            filters['Sub Brand'] = [s for s in kpi_template['Sub Brand'].split(', ')]
        if kpi_template['PRICE SEGMENT']:
            filters['PRICE SEGMENT'] = kpi_template['PRICE SEGMENT']
        if kpi_template['Segment']:
            filters['SEGMENT'] = [s for s in kpi_template['Segment'].split(', ')]
        if kpi_template['Brand']:
            filters['brand_name'] = [s for s in kpi_template['Brand'].split(', ')]
        if kpi_template['NATURALS']:
            filters['NATURALS'] = kpi_template['NATURALS']
        if kpi_template['P&G BRAND']:
            filters['P&G BRAND'] = kpi_template['P&G BRAND']
        if kpi_template['GENDER']:
            filters['GENDER'] = kpi_template['GENDER']
        if kpi_template['TYPE']:
            filters['TYPE'] = kpi_template['TYPE']
        if kpi_template['FORM']:
            filters['FORM'] = kpi_template['FORM']
        new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
        result = self.tools.calculate_share_space_length(**filters)
        score = result * self.MM_TO_FEET_CONVERSION
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)
        if return_result:
            self.related_kpi_results[kpi_name] = score

    @kpi_runtime()
    def calculate_linear_feet_new(self, kpi_set_fk, kpi_name, scene_types, category):
        template = self.linear_feet_data.loc[self.linear_feet_data['KPI name'] == kpi_name]
        kpi_template = template.loc[(template['KPI name'] == kpi_name) & (template['category'] == category)]
        if kpi_template.empty:
            return None
        values_to_check = []
        secondary_values_to_check = []
        kpi_template = kpi_template.iloc[0]
        exclude_pl = False
        filters = {'template_name': scene_types, 'category': kpi_template['category']}
        if kpi_template['category'] in FABRICARE_CATEGORIES:
            category_att = PG_CATEGORY
        else:
            category_att = 'category'
        if kpi_template['filter_1']:
            values_to_check = self.all_products.loc[(self.all_products[category_att] == kpi_template['category'])
                                                    & (self.all_products['product_type'] == 'SKU')][
                kpi_template['filter_1']].unique().tolist()
        # if kpi_template['filter_2']:
        #     secondary_values_to_check = self.all_products.loc[(self.all_products['category'] == kpi_template['category'])
        #                                                       & (self.all_products['product_type'] == 'SKU')][
        #         kpi_template['filter_2']].unique().tolist()

        for primary_filter in values_to_check:
            filters[kpi_template['filter_1']] = primary_filter
            if primary_filter is None:
                continue
            if kpi_template['filter_2']:
                secondary_values_to_check = \
                    self.all_products.loc[(self.all_products[category_att] == kpi_template['category']) &
                                          (self.all_products[kpi_template['filter_1']] == primary_filter)
                                          & (self.all_products['product_type'] == 'SKU')][
                        kpi_template['filter_2']].unique().tolist()
            if secondary_values_to_check:
                for secondary_filter in secondary_values_to_check:
                    if self.all_products[(self.all_products[kpi_template['filter_1']] == primary_filter) &
                            (self.all_products[
                                 kpi_template['filter_2']] == secondary_filter)].empty or secondary_filter is None:
                        continue

                    filters[kpi_template['filter_2']] = secondary_filter
                    filtered_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
                    filtered_df2 = filtered_df[self.tools.get_filter_condition(filtered_df, **filters)]
                    new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                    if kpi_template['category'] in FABRICARE_CATEGORIES:
                        filters[PG_CATEGORY] = kpi_template['category']
                        if kpi_template['filter_2'] == 'SEGMENT':
                            exclude_pl = True
                        if 'category' in filters.keys():
                            del filters['category']
                    if filtered_df2.empty:
                        result = 0
                    else:
                        # result = self.tools.calculate_share_space_length(**filters)
                        result = self.tools.calculate_share_space_length_new(exclude_pl=exclude_pl, **filters)
                    score = result * self.MM_TO_FEET_CONVERSION
                    filters['category'] = kpi_template['category']
                    self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3, result=score,
                                            score=score)
            else:
                new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                if kpi_template['category'] in FABRICARE_CATEGORIES:
                    filters[PG_CATEGORY] = kpi_template['category']
                    if kpi_template['filter_1'] == 'SEGMENT':
                        exclude_pl = True
                    if 'category' in filters.keys():
                        del filters['category']
                filtered_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
                if filtered_df.empty:
                    result = 0
                else:
                    # result = self.tools.calculate_share_space_length(**filters)
                    result = self.tools.calculate_share_space_length_new(exclude_pl=exclude_pl, **filters)
                score = result * self.MM_TO_FEET_CONVERSION
                filters['category'] = kpi_template['category']
                self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3, result=score, score=score)

    @kpi_runtime()
    def calculate_category_space(self, kpi_set_fk, kpi_name, scene_types, category):
        template = self.category_space_data.loc[(self.category_space_data['KPI name'] == kpi_name) &
                                                (self.category_space_data['category'] == category)]
        kpi_template = template.loc[template['KPI name'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        values_to_check = []
        secondary_values_to_check = []
        exclude_pl_wo_pg_category = False
        filters = {'template_name': scene_types, 'category': kpi_template['category']}
        if kpi_template['category'] in FABRICARE_CATEGORIES:
            category_att = PG_CATEGORY
        else:
            category_att = 'category'
        if kpi_template['filter_1']:
            values_to_check = self.all_products.loc[self.all_products[category_att] == kpi_template['category']][
                kpi_template['filter_1']].unique().tolist()
        if kpi_template['filter_2']:
            secondary_values_to_check = \
                self.all_products.loc[self.all_products[category_att] == kpi_template['category']][
                    kpi_template['filter_2']].unique().tolist()
        for primary_filter in values_to_check:
            filters[kpi_template['filter_1']] = primary_filter
            if secondary_values_to_check:
                for secondary_filter in secondary_values_to_check:
                    if self.all_products[(self.all_products[kpi_template['filter_1']] == primary_filter) &
                            (self.all_products[
                                 kpi_template['filter_2']] == secondary_filter)].empty or secondary_filter is None:
                        continue
                    filters[kpi_template['filter_2']] = secondary_filter
                    new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                    if kpi_template['category'] in FABRICARE_CATEGORIES:
                        exclude_pl_wo_pg_category = True
                        filters[PG_CATEGORY] = kpi_template['category']
                        if 'category' in filters.keys():
                            del filters['category']
                    result = self.tools.calculate_category_space(new_kpi_name, exclude_pl=exclude_pl_wo_pg_category,
                                                                 **filters)
                    filters['category'] = kpi_template['category']
                    # score = result * self.MM_TO_FEET_CONVERSION
                    score = result
                    self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3, result=score,
                                            score=score)
            else:
                new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                if kpi_template['category'] in FABRICARE_CATEGORIES:
                    exclude_pl_wo_pg_category = True
                    filters[PG_CATEGORY] = kpi_template['category']
                    del filters['category']
                result = self.tools.calculate_category_space(new_kpi_name, exclude_pl=exclude_pl_wo_pg_category, **filters)
                filters['category'] = kpi_template['category']
                # score = result * self.MM_TO_FEET_CONVERSION
                score = result
                self.write_to_db_result(kpi_set_fk, kpi_name=new_kpi_name, level=self.LEVEL3, result=score, score=score)

    @kpi_runtime()
    def calculate_eye_level(self, kpi_set_fk, kpi_name, scene_type, category=None, list_type=False):
        if set(self.scif['template_name'].unique().tolist()) & set(scene_type):
            if list_type:
                kpi_template = self.eye_level_data.loc[self.eye_level_data['kpi group'] == kpi_name]
            else:
                kpi_template = self.eye_level_data.loc[self.eye_level_data['KPI name'] == kpi_name]
            # kpi_template = self.eye_level_data.loc[self.eye_level_data['KPI name'] == kpi_name]
            kpi_template = kpi_template.iloc[0]
            filters = {'template_name': scene_type,
                       'category': kpi_template['category']}
            if ',' in kpi_template['SEGMENT']:
                filters['SEGMENT'] = kpi_template['SEGMENT'].split(', ')
            if kpi_template['brand_name']:
                filters['brand_name'] = kpi_template['brand_name']
            if kpi_template['Sub Brand']:
                filters['Sub Brand'] = kpi_template['Sub Brand']
            if kpi_template['manufacturer_name']:
                filters['manufacturer_name'] = kpi_template['manufacturer_name']
            if category:
                filters['category'] = category
            list_result = False
            # eye_level_definition = self.eye_level_definition.loc[(self.eye_level_definition['retailer'] == self.retailer) &
            #                                                     (self.eye_level_definition['category'] == category)]
            if kpi_template['percentage']:
                result = self.tools.calculate_eye_level_assortment(eye_level_configurations=self.eye_level_definition,
                                                                   min_number_of_products=1, percentage_result=True,
                                                                   requested_attribute='facings', **filters)
                if result[1]:
                    score = (result[0] / float(result[1])) * 100
                else:
                    score = 0
            elif list_type:
                result = self.tools.calculate_eye_level_assortment(eye_level_configurations=self.eye_level_definition,
                                                                   min_number_of_products=1, products_list=True,
                                                                   **filters)
                score = 0
                if result:
                    list_result = True
            else:
                result = self.tools.calculate_eye_level_assortment(eye_level_configurations=self.eye_level_definition,
                                                                   min_number_of_products=1, products_list=True,
                                                                   **filters)
                score = 1 if result[0] >= 1 else 0
            if not list_result:
                self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)
            else:
                kpi_template = self.eye_level_data.loc[self.eye_level_data['kpi group'] == kpi_name]
                kpi_names = kpi_template['KPI name'].unique().tolist()
                # score_dict = {}
                # for result in list(set(result)):
                #     score_dict[result] = result.count(result)
                i = 0
                for product_name in result:
                    product_name = product_name.replace("'", "\\'")
                    self.write_to_db_result(kpi_set_fk, kpi_name=kpi_names[i], level=self.LEVEL3, result=product_name,
                                            score=None)
                    i += 1
                    if i > 50:
                        break
                return

    @kpi_runtime()
    def calculate_eye_level_new(self, kpi_set_fk, kpi_name, scene_type, category, list_type=False):
        if set(self.scif['template_name'].unique().tolist()) & set(scene_type):
            if list_type:
                kpi_template = self.eye_level_data.loc[self.eye_level_data['kpi group'] == kpi_name]
            else:
                kpi_template = self.eye_level_data.loc[(self.eye_level_data['KPI name'] == kpi_name) &
                                                       (self.eye_level_data['category'] == category)]
            # kpi_template = self.eye_level_data.loc[self.eye_level_data['KPI name'] == kpi_name]
            kpi_template = kpi_template.iloc[0]
            values_to_check = []
            secondary_values_to_check = []
            filters = {'template_name': scene_type, 'category': kpi_template['category']}
            if kpi_template['category'] in FABRICARE_CATEGORIES:
                category_att = PG_CATEGORY
            else:
                category_att = 'category'
            if kpi_template['filter_1']:
                values_to_check = self.all_products.loc[(self.all_products[category_att] == kpi_template['category'])
                                                        & (self.all_products['product_type'] == 'SKU')][
                    kpi_template['filter_1']].unique().tolist()
            # if kpi_template['filter_2']:
            #     secondary_values_to_check = self.all_products.loc[
            #         (self.all_products[category_att] == kpi_template['category'])
            #         & (self.all_products['product_type'] == 'SKU')][
            #         kpi_template['filter_2']].unique().tolist()

            for primary_filter in values_to_check:
                filters[kpi_template['filter_1']] = primary_filter
                if kpi_template['filter_2']:
                    secondary_values_to_check = \
                        self.all_products.loc[(self.all_products[category_att] == kpi_template['category']) &
                                              (self.all_products[kpi_template['filter_1']] == primary_filter)
                                              & (self.all_products['product_type'] == 'SKU')][
                            kpi_template['filter_2']].unique().tolist()
                if secondary_values_to_check:
                    for secondary_filter in secondary_values_to_check:
                        if self.all_products[(self.all_products[kpi_template['filter_1']] == primary_filter) &
                                (self.all_products[kpi_template['filter_2']] == secondary_filter)].empty:
                            continue
                        filters[kpi_template['filter_2']] = secondary_filter
                        new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                        if kpi_template['category'] in FABRICARE_CATEGORIES:
                            filters[PG_CATEGORY] = kpi_template['category']
                            category = 'FABRICARE'
                            if 'category' in filters.keys():
                                del filters['category']
                        self.eye_level_writer(kpi_template, kpi_set_fk, new_kpi_name, filters, list_type,
                                              category=category)
                        filters['category'] = kpi_template['category']
                else:
                    new_kpi_name = self.kpi_name_builder(kpi_name, **filters)
                    if kpi_template['category'] in FABRICARE_CATEGORIES:
                        filters[PG_CATEGORY] = kpi_template['category']
                        category = 'FABRICARE'
                        if 'category' in filters.keys():
                            del filters['category']
                    self.eye_level_writer(kpi_template, kpi_set_fk, new_kpi_name, filters, list_type,
                                          category=category)
                    filters['category'] = kpi_template['category']

    def eye_level_writer(self, kpi_template, kpi_set_fk, kpi_name, filters, list_type, category=None):
        list_result = False
        retailer = False
        sub_category = False
        if category is not None:
            if category in ['FEM CARE', 'AI']:
                retailer = True
                if category == 'FEM CARE':
                    sub_category = True
            eye_level_definition = self.eye_level_mapping[category]
            if retailer:
                eye_level_definition = eye_level_definition.loc[eye_level_definition['retailer'] == self.retailer]
        else:
            eye_level_definition = self.eye_level_definition

        if kpi_template['percentage']:
            result = self.tools.calculate_eye_level_assortment(eye_level_configurations=eye_level_definition,
                                                               min_number_of_products=1, percentage_result=True,
                                                               sub_category=sub_category,
                                                               requested_attribute='facings', **filters)
            if result[1]:
                score = (result[0] / float(result[1])) * 100
            else:
                score = None
            self.eye_level_results[kpi_name] = score
        elif list_type:
            result = self.tools.calculate_eye_level_assortment(eye_level_configurations=eye_level_definition,
                                                               category=category, sub_category=sub_category,
                                                               min_number_of_products=1, products_list=True, **filters)
            score = 0
            if result:
                list_result = True
        else:
            result = self.tools.calculate_eye_level_assortment(eye_level_configurations=eye_level_definition,
                                                               category=category, sub_category=sub_category,
                                                               min_number_of_products=1, products_list=False, **filters)
            score = 1 if result[0] >= 1 else None
        if not list_result:
            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)
        else:
            # kpi_template = self.eye_level_data.loc[self.eye_level_data['kpi group'] == kpi_name]
            # kpi_names = kpi_template['KPI name'].unique().tolist()
            # score_dict = {}
            # for result in list(set(result)):
            #     score_dict[result] = result.count(result)
            i = 0
            for product_name in result:
                product_name = product_name.replace("'", "\\'")
                # self.write_to_db_result(kpi_set_fk, kpi_name=kpi_names[i], level=self.LEVEL3, result=product_name,
                #                         score=None)
                self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name + i, level=self.LEVEL3, result=product_name,
                                        score=None)
                i += 1
                if i > 50:
                    break
            return

    def calculate_naturals_adjacency(self, kpi_set_fk, scene_type, kpi_name):
        pass

    def calculate_sud_orchestration(self, kpi_set_fk, scene_type, kpi_name):
        eye_level_kpi_name = 'Location:Eye_Level:TOTAL LAUNDRY CARE:SEG=LAUNDRY CARE'
        block_kpi_name = 'Blocking:Prod_lvl_Blocking:TOTAL LAUNDRY CARE:SEG=LAUNDRY CARE:FORM=LAUNDRY UNIT DOSE'
        result = 0
        if set(self.scif['template_name'].unique().tolist()) & set(scene_type):
            if eye_level_kpi_name in self.eye_level_results.keys() and block_kpi_name in self.hor_ver_results.keys():
                if self.eye_level_results[eye_level_kpi_name] > 0 and self.hor_ver_results[block_kpi_name] == 'HORIZONTAL':
                    result = 1

            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=result, score=result)

    def calculate_fabricare_regimen(self, kpi_set_fk, scene_type, kpi_name):
            block_kpi_name1 = 'Blocking:Prod_lvl_Blocking:TOTAL FABRIC CONDITIONERS:SEG=FABRIC CONDITIONERS:FORM=FABRIC CONDITIONER - LIQUID'
            block_kpi_name2 = 'Blocking:Prod_lvl_Blocking:TOTAL FABRIC CONDITIONERS:SEG=FABRIC CONDITIONERS:FORM=FABRIC CONDITIONER - SHEETS'
            block_kpi_name3 = 'Blocking:Prod_lvl_Blocking:TOTAL FABRIC CONDITIONERS:SEG=FABRIC CONDITIONERS:FORM=FABRIC CONDITIONER - BEADS'
            result = 0
            if set(self.scif['template_name'].unique().tolist()) & set(scene_type):
                if block_kpi_name1 in self.block_results.keys() and block_kpi_name2 in self.block_results.keys() and \
                                block_kpi_name3 in self.block_results.keys():
                        if self.block_results[block_kpi_name1] and self.block_results[block_kpi_name2] and self.block_results[block_kpi_name3]:
                            result = 1

                self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=result, score=result)

    def calculate_auto_assortment_compliance(self):
            auto_assortment = AutoAssortmentHandler()
            current_store_assortment = auto_assortment.get_current_assortment_per_store(self.store_id, self.visit_date)
            store_pskus = self.get_power_skus_per_store()
            distributed_products = self.scif[(self.scif['product_fk'].isin(current_store_assortment)) &
                                             (self.scif['dist_sc'] == 1)]['product_fk'].unique().tolist()
            # psku_distributed_products = self.scif[(self.scif['product_fk'].isin(current_store_assortment)) &
            #                                       (self.scif['dist_sc'] == 1) &
            #                                       (self.scif['POWER_SKU'] == 'Y')]['product_fk'].unique().tolist()
            psku_distributed_products = self.scif[(self.scif['product_fk'].isin(current_store_assortment)) &
                                                  (self.scif['dist_sc'] == 1) &
                                                  (self.scif['product_fk'].isin(store_pskus))][
                'product_fk'].unique().tolist()
            # psku_products = self.all_products[self.all_products['POWER_SKU'] == 'Y']['product_fk'].unique().tolist()
            if current_store_assortment:
                availability = (len(distributed_products) / float(len(current_store_assortment))) * 100
            else:
                availability = 0
            osa_oos = 100 - availability
            if current_store_assortment:
                psku_availability = len(psku_distributed_products) / float(len(current_store_assortment)) * 100
            else:
                psku_availability = 0
            psku_oos = 100 - psku_availability
            if store_pskus:
                d_void = (len(set(store_pskus).difference(set(current_store_assortment))) / float(
                    len(store_pskus))) * 100
            else:
                d_void = 0
            osa_kpi_set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == 'OSA']['kpi_set_fk'].values[0]
            dvoid_kpi_set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == 'D-VOID']['kpi_set_fk'].values[
                0]

            self.write_to_db_result(osa_kpi_set_fk, result=None, level=self.LEVEL1)
            # self.write_to_db_result(osa_kpi_set_fk, kpi_name='OSA', result=osa_oos, threshold=availability,
            #                         level=self.LEVEL3)
            self.save_assortment_raw_data(current_store_assortment, distributed_products, osa_kpi_set_fk)
            self.write_to_db_result(dvoid_kpi_set_fk, result=None, level=self.LEVEL1)
            # self.write_to_db_result(dvoid_kpi_set_fk, kpi_name='D-VOID', result=d_void, threshold=0, level=self.LEVEL3)
            self.save_assortment_raw_data(store_pskus, psku_distributed_products, dvoid_kpi_set_fk)

    def calculate_auto_assortment_compliance_per_category(self):
        auto_assortment = AutoAssortmentHandler()
        current_store_assortment = auto_assortment.get_current_assortment_per_store(self.store_id, self.visit_date)
        store_pskus = self.get_power_skus_per_store()
        for category in self.scif['category'].unique().tolist():
            if category not in CATEGORY_OSA_MAPPING.keys() or category not in self.scif[
                'template_group'].unique().tolist():
                continue
            current_category_assortment = self.all_products[
                (self.all_products['product_fk'].isin(current_store_assortment)) &
                (self.all_products['category'] == category)]['product_fk'].unique().tolist()
            distributed_products = self.scif[(self.scif['product_fk'].isin(current_store_assortment)) &
                                             (self.scif['dist_sc'] == 1) &
                                             (self.scif['category'] == category)]['product_fk'].unique().tolist()
            psku_distributed_products = self.scif[(self.scif['dist_sc'] == 1) &
                                                  (self.scif['product_fk'].isin(store_pskus)) &
                                                  (self.scif['category'] == category)]['product_fk'].unique().tolist()
            category_osa_set = CATEGORY_OSA_MAPPING[category]
            category_osa_kpi_set_fk = \
                self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == category_osa_set]['kpi_set_fk'].values[
                    0]
            category_dvoid_set = CATEGORY_DVOID_MAPPING[category]
            category_dvoid_kpi_set_fk = \
                self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == category_dvoid_set]['kpi_set_fk'].values[
                    0]
            psku_products = self.all_products[(self.all_products['product_fk'].isin(store_pskus))
                                              & (self.all_products['category'] == category)][
                'product_fk'].unique().tolist()
            if current_category_assortment:
                availability = (len(distributed_products) / float(len(current_category_assortment))) * 100
            else:
                availability = 0
            osa_oos = 100 - availability
            if len(current_category_assortment) == 0:
                osa_oos = None
            if psku_products:
                pskus_in_ass_not_in_visit = set(current_category_assortment) & (
                    set(psku_products).difference(set(psku_distributed_products)))
                psku_availability = len(psku_distributed_products) / float(len(psku_products)) * 100
            else:
                psku_availability = 0
                pskus_in_ass_not_in_visit = 0
            psku_oos = 100 - psku_availability
            if psku_products:
                if len(current_category_assortment) > 0:
                    # d_void = (len(set(psku_products).difference(set(current_category_assortment))) / float(
                    #     len(psku_products))) * 100
                    d_void = 100 - ((len(psku_distributed_products) + len(pskus_in_ass_not_in_visit)) / float(
                        len(psku_products))) * 100
                else:
                    d_void = None
                    d_void_new = None
            else:
                d_void = 0
                d_void_new = 0

            # self.write_to_db_result(osa_aggregation_kpi_set_fk, kpi_name=category, result=osa_oos, threshold=availability,
            #                         level=self.LEVEL3)
            self.write_to_db_result(category_osa_kpi_set_fk, kpi_name=category_osa_set, result=osa_oos,
                                    threshold=availability,
                                    level=self.LEVEL3)
            # self.write_to_db_result(dvoid_aggregation_kpi_set_fk, kpi_name=category, result=d_void, threshold=0, level=self.LEVEL3)
            self.write_to_db_result(category_dvoid_kpi_set_fk, kpi_name=category_dvoid_set, result=d_void, threshold=0,
                                    level=self.LEVEL3)
            self.write_to_db_result(category_osa_kpi_set_fk, result=None, level=self.LEVEL1)
            self.write_to_db_result(category_dvoid_kpi_set_fk, result=None, level=self.LEVEL1)

            self.save_assortment_raw_data(current_category_assortment, distributed_products, category_osa_kpi_set_fk)
            self.save_assortment_raw_data(store_pskus, psku_distributed_products, category_dvoid_kpi_set_fk)


            # self.write_to_db_result(osa_aggregation_kpi_set_fk, result=None, level=self.LEVEL1)
            # self.write_to_db_result(dvoid_aggregation_kpi_set_fk, result=None, level=self.LEVEL1)

    def calculate_auto_assortment_compliance_per_brand(self):
        auto_assortment = AutoAssortmentHandler()
        current_store_assortment = auto_assortment.get_current_assortment_per_store(self.store_id, self.visit_date)
        store_pskus = self.get_power_skus_per_store()
        for brand in self.scif['brand_name'].unique().tolist():
            brand_categories = self.all_products[self.all_products['brand_name'] == brand]['category'].unique().tolist()
            if not set(brand_categories) & set(CATEGORY_OSA_MAPPING.keys()) or not set(brand_categories) & set(
                    self.scif['template_group'].unique().tolist()):
                continue
            current_brand_assortment = self.all_products[
                (self.all_products['product_fk'].isin(current_store_assortment)) &
                (self.all_products['brand_name'] == brand)]['product_fk'].unique().tolist()
            distributed_products = self.scif[(self.scif['product_fk'].isin(current_store_assortment)) &
                                             (self.scif['dist_sc'] == 1) &
                                             (self.scif['brand_name'] == brand)]['product_fk'].unique().tolist()
            psku_distributed_products = self.scif[(self.scif['dist_sc'] == 1) &
                                                  (self.scif['product_fk'].isin(store_pskus)) &
                                                  (self.scif['brand_name'] == brand)]['product_fk'].unique().tolist()
            # category_osa_set = CATEGORY_OSA_MAPPING[category]
            # category_osa_kpi_set_fk = \
            #     self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == category_osa_set]['kpi_set_fk'].values[
            #         0]
            # category_dvoid_set = CATEGORY_DVOID_MAPPING[category]
            # category_dvoid_kpi_set_fk = \
            #     self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == category_dvoid_set]['kpi_set_fk'].values[
            #         0]
            osa_kpi_set_fk = \
                self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == OSA]['kpi_set_fk'].values[
                    0]
            dvoid_kpi_set_fk = \
                self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == DVOID]['kpi_set_fk'].values[
                    0]
            psku_products = self.all_products[(self.all_products['product_fk'].isin(store_pskus))
                                              & (self.all_products['brand_name'] == brand)][
                'product_fk'].unique().tolist()
            if current_brand_assortment:
                availability = (len(distributed_products) / float(len(current_brand_assortment))) * 100
            else:
                availability = 0
            osa_oos = 100 - availability
            if len(current_brand_assortment) == 0:
                osa_oos = None
            if psku_products:
                pskus_in_ass_not_in_visit = set(current_brand_assortment) & (
                    set(psku_products).difference(set(psku_distributed_products)))
                psku_availability = len(psku_distributed_products) / float(len(psku_products)) * 100
            else:
                psku_availability = 0
                pskus_in_ass_not_in_visit = 0
            psku_oos = 100 - psku_availability
            if psku_products:
                if len(current_brand_assortment) > 0:
                    # d_void = (len(set(psku_products).difference(set(current_category_assortment))) / float(
                    #     len(psku_products))) * 100
                    d_void = 100 - ((len(psku_distributed_products) + len(pskus_in_ass_not_in_visit)) / float(
                        len(psku_products))) * 100
                else:
                    d_void = None
                    d_void_new = None
            else:
                d_void = 0
                d_void_new = 0
            # brand_osa_name = OSA + ' ' + brand
            brand_osa_name = OSA.format(category=brand_categories, brand=brand)

            # self.write_to_db_result(osa_aggregation_kpi_set_fk, kpi_name=category, result=osa_oos, threshold=availability,
            #                         level=self.LEVEL3)
            self.write_to_db_result(osa_kpi_set_fk, kpi_name=brand_osa_name, result=osa_oos, threshold=availability,
                                    level=self.LEVEL3)
            # self.write_to_db_result(dvoid_aggregation_kpi_set_fk, kpi_name=category, result=d_void, threshold=0, level=self.LEVEL3)
            self.write_to_db_result(dvoid_kpi_set_fk, kpi_name=brand_osa_name, result=d_void, threshold=0,
                                    level=self.LEVEL3)
            self.write_to_db_result(osa_kpi_set_fk, result=None, level=self.LEVEL1)
            self.write_to_db_result(dvoid_kpi_set_fk, result=None, level=self.LEVEL1)

            self.save_assortment_raw_data(current_brand_assortment, distributed_products, osa_kpi_set_fk)
            self.save_assortment_raw_data(store_pskus, psku_distributed_products, dvoid_kpi_set_fk)

    def get_power_skus_per_store(self):
        store_power_skus = self.power_skus[self.power_skus['Retailer'] == self.retailer]['pk'].unique().tolist()
        pskus_pks = []
        for pk in store_power_skus:
            if len(pk) > 0:
                int_pk = int(float(pk))
                pskus_pks.append(int_pk)
        store_power_skus_fks = self.all_products[
            self.all_products['product_fk'].isin(pskus_pks)]['product_fk'].unique().tolist()

        return store_power_skus_fks

    def save_assortment_raw_data(self, assortment_list, dist_prod_list, kpi_set_fk):
        for product in assortment_list:
            oos_result = 1
            # if product in dist_prod_list:
            #     oos_result = 0
            # try:
            #     kpi_name = self.all_products[self.all_products['product_fk'] == product]['product_ean_code'].values[0]
            #     if kpi_name is not None:
            #         self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, result=oos_result, threshold=1,
            #                                 level=self.LEVEL3)
            #     else:
            #         kpi_name = 'P - {}'.format(product)
            #         self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, result=oos_result, threshold=1,
            #                                 level=self.LEVEL3)
            # except Exception as e:
            #     kpi_name = 'P - {}'.format(product)
            #     if kpi_name is not None:
            #         self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, result=oos_result, threshold=1,
            #                                 level=self.LEVEL3)
            #     Log.info('Product pk {} has no EAN code'.format(product))
            #     continue
            if product in dist_prod_list:
                oos_result = 0
            try:
                kpi_name = self.all_products[self.all_products['product_fk'] == product]['product_ean_code'].values[0]
                if kpi_name is not None:
                    self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, result=oos_result, threshold=1,
                                            level=self.LEVEL3)
                else:
                    kpi_name = 'P - {}'.format(product)
                    self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, result=oos_result, threshold=1,
                                            level=self.LEVEL3)
            except Exception as e:
                kpi_name = 'P - {}'.format(product)
                if kpi_name is not None:
                    self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, result=oos_result, threshold=1,
                                            level=self.LEVEL3)
                Log.info('Product pk {} has no EAN code'.format(product))
                continue

    def check_products_on_top_shelf(self, kpi_set_fk, kpi_name, scene_type):
        kpi_template = self.count_of_data.loc[self.count_of_data['KPI name'] == kpi_name]  # todo: change this
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        if kpi_template.empty:
            return None
        scif_scenes = self.scif.loc[self.scif['template_name'].isin(scene_type)]
        scenes = scif_scenes['scene_id'].unique().tolist()
        if scenes is not None:
            filters = {'scene_id': scenes, 'shelf_number': 1}
            if kpi_template['Brand']:
                filters['brand_name'] = kpi_template['Brand']

            result = self.tools.calculate_assortment(assortment_entity=kpi_template['count'], **filters)
            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=result,
                                    score=int(result))
            score = True if result > 0 else False
            self.related_kpi_results[kpi_name] = score

    def calculate_bookend(self, kpi_set_fk, kpi_name, scene_type):
        kpi_template = self.bookend_data.loc[self.bookend_data['KPI name'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        filters_1 = {kpi_template['filter_1']: kpi_template['filter_1_value']}
        filters_2 = {kpi_template['filter_2']: kpi_template['filter_2_value']}
        score_count = 0
        for filter_option in [filters_1, filters_2]:
            # self.calculate_anchor_new(kpi_set_fk, kpi_template['filter_1_value'] + ' anchor', scene_type, filters=filter_option,
            #                           return_result=True)
            #
            # self.calculate_anchor_new(kpi_set_fk, kpi_template['filter_2_value'] + ' reverse', scene_type,
            #                           filters=filter_option, return_result=True)
            # if self.related_kpi_results[kpi_template['filter_1_value'] + ' anchor'] or self.related_kpi_results[
            #             kpi_template['filter_1_value'] + ' reverse']:
            #     score_count += 1
            anchor_result = self.calculate_anchor_stand_alone(scene_type, kpi_template['filter_1_value'] + ' anchor',
                                                              kpi_template['category'], filters=filter_option)
            rev_result = self.calculate_anchor_stand_alone(scene_type, kpi_template['filter_1_value'] + ' reverse',
                                                           kpi_template['category'], filters=filter_option)
            if anchor_result or rev_result:
                score_count += 1
        result = 1 if score_count >= 2 else 0
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=result,
                                score=result)

    def calculate_anchor_stand_alone(self, scene_type, kpi_name, category, filters):
        list_type = False
        score = 0
        for s_type in scene_type:
            if 'LL' in s_type:
                position = 'left'
                if 'rev' in kpi_name:
                    position = 'right'
            else:
                position = 'right'
                if 'rev' in kpi_name:
                    position = 'left'
            scif_scenes = self.scif.loc[self.scif['template_name'] == s_type]['scene_id'].unique().tolist()
            filters['scene_id'] = scif_scenes
            results = self.tools.calculate_products_on_edge(list_result=list_type, position=position,
                                                            category=category, **filters)
            if results[0] >= 1:
                score = 1
        return score

    def calculate_color_wheel(self, kpi_set_fk, kpi_name, scene_type):
        kpi_template = self.count_of_data.loc[self.count_of_data['KPI name'] == kpi_name]  # todo: change this
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        if kpi_template.empty:
            return None
        filters = {}
        if kpi_template['sub_category']:
            filters['sub_category'] = kpi_template['sub_category']
        if kpi_template['SEGMENT']:
            filters['SEGMENT'] = kpi_template['SEGMENT']
        if kpi_template['brand_name']:
            filters['brand_name'] = kpi_template['brand_name']
        if kpi_template['category']:
            filters['category'] = kpi_template['category']

        filters['template_name'] = scene_type
        include_empty = False
        try:
            res = self.tools.calculate_block_together(include_empty=include_empty, minimum_block_ratio=0.75,
                                                      color_wheel=True, **filters)
        except Exception as e:
            res = False

        if res:
            collections_dict = {}
            if res['scene_match_fk']:
                relevant_matches = self.match_product_in_scene[
                    self.match_product_in_scene['scene_match_fk'].isin(res['scene_match_fk'])]
                relevant_matches.sort_values(['bay_number', 'shelf_number'])
                i = 1
                for match in relevant_matches:
                    product_fk = self.match_product_in_scene[self.match_product_in_scene['scene_match_fk'] == match][
                        'product_fk'].values[0]
                    product_collection = self.all_products[self.all_products['product_fk'] == product_fk][
                        'COLLECTION'].values[0]
                    if product_collection not in collections_dict.values():
                        position_name = 'Position {}'.format(i)
                        collections_dict[position_name] = product_collection
                        i += 1
                for position in collections_dict.keys():
                    self.write_to_db_result(kpi_set_fk, kpi_name=position, level=self.LEVEL3,
                                            result=collections_dict[position], score=collections_dict[position])
        self.related_kpi_results[kpi_name] = res

    def calculate_facing_sos(self, kpi_name, nominator_filters, denominator_filters):
        """
        This function calculates Facings SOS
        """
        sos_filters = nominator_filters
        filters = {}
        for filter_key in denominator_filters.keys():
            filters[filter_key] = denominator_filters[filter_key]
        result = self.tools.calculate_share_of_shelf(sos_filters=sos_filters, **filters)

        self.related_kpi_results[kpi_name] = result

        return result

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

    def herbal_essences_color_wheel(self, kpi_set_fk, kpi_name, scene_type):
        kpi_template = self.he_template
        score = True
        for i, row in kpi_template.iterrows():
            if row['type'] == 'facings sos':
                kpi_data = self.sos_template[self.sos_template['name'] == row['name']]
                nominator_filters = {kpi_data['nominator_param']: kpi_data['nominator_value']}
                denominator_filters = {kpi_data['denominator_param']: kpi_data['denominator_value'],
                                       'template_name': scene_type}
                self.calculate_facing_sos(row['name'], nominator_filters, denominator_filters)
            elif row['type'] == 'top shelf':
                self.check_products_on_top_shelf(kpi_set_fk, row['name'], scene_type)
            elif row['type'] in ('Vertical Block', 'regular block'):
                self.calculate_block_together(kpi_set_fk, row['name'], scene_type)
            elif row['type'] == 'color wheel':
                self.calculate_color_wheel(kpi_set_fk, row['name'], scene_type)
            elif row['type'] == 'Orchestration':
                self.calculate_orchestrated(kpi_set_fk, row['name'], scene_type)
            if row['name'] in self.related_kpi_results.keys():
                result = self.related_kpi_results[row['name']]
            else:
                result = False
            if not result:
                score = False
            self.write_to_db_result(kpi_set_fk, result=None, level=self.LEVEL3, kpi_name=kpi_name)
        final_score = 100 if score else 0
        self.write_to_db_result(kpi_set_fk, result=final_score, level=self.LEVEL3, kpi_name=kpi_name)

    # def head_and_shoulders_solution_center(self, kpi_set_fk, kpi_name, scene_type):
    #
    #     #TODO the code below will fail
    #     kpi_template = ' ' #self.hns_template
    #     score = True
    #     for i, row in kpi_template.iterrows():
    #         if row['type'] == 'anchor':
    #             self.calculate_anchor(kpi_set_fk, row['name'], scene_type, return_result=True)
    #         elif row['type'] == 'top shelf':
    #             self.check_products_on_top_shelf(kpi_set_fk, row['name'], scene_type)
    #         elif row['type'] in ('Vertical Block', 'regular block', 'horizontally block'):
    #             self.calculate_block_together(kpi_set_fk, row['name'], scene_type, return_result=True)
    #         elif row['type'] == 'linear feet':
    #             self.calculate_linear_feet(kpi_set_fk, row['name'], scene_type)
    #         elif row['type'] == 'eye level':
    #             self.calculate_eye_level(kpi_set_fk, row['name'], scene_type)
    #         elif row['type'] == 'relative position':
    #             self.calculate_relative_position(kpi_set_fk, row['name'], scene_type)
    #         elif row['type'] == 'Orchestration':
    #             self.calculate_orchestrated(kpi_set_fk, row['name'], scene_type)
    #         if row['name'] in self.related_kpi_results.keys():
    #             result = self.related_kpi_results[row['name']]
    #         else:
    #             result = False
    #         if not result:
    #             score = False
    #         self.write_to_db_result(kpi_set_fk, result=None, level=self.LEVEL3, kpi_name=kpi_name)
    #     final_score = 100 if score else 0
    #     self.write_to_db_result(kpi_set_fk, result=final_score, level=self.LEVEL3, kpi_name=kpi_name)

    def pantene_golden_strategy(self, kpi_set_fk, kpi_name, scene_type):
        kpi_template = self.pantene_template
        score = True
        for i, row in kpi_template.iterrows():
            if row['type'] == 'anchor':
                self.calculate_anchor(kpi_set_fk, row['name'], scene_type)
            elif row['type'] == 'top shelf':
                self.check_products_on_top_shelf(kpi_set_fk, row['name'], scene_type)
            elif row['type'] in ('Vertical Block', 'regular block', 'horizontally block'):
                self.calculate_block_together(kpi_set_fk, row['name'], scene_type)
            elif row['type'] == 'linear feet':
                self.calculate_linear_feet(kpi_set_fk, row['name'], scene_type)
            elif row['type'] == 'eye level':
                self.calculate_eye_level(kpi_set_fk, row['name'], scene_type)
            elif row['type'] == 'relative position':
                self.calculate_relative_position(kpi_set_fk, row['name'], scene_type)
            elif row['type'] == 'availability':
                self.calculate_availability(kpi_set_fk, row['name'], scene_type)
            elif row['type'] == 'Orchestration':
                self.calculate_orchestrated(kpi_set_fk, row['name'], scene_type)
            if row['name'] in self.related_kpi_results.keys():
                result = self.related_kpi_results[row['name']]
            else:
                result = 0
            if not result:
                score = False
            self.write_to_db_result(kpi_set_fk, result=result, level=self.LEVEL3, kpi_name=row['name'])
        final_score = 100 if score else 0
        self.write_to_db_result(kpi_set_fk, result=final_score, level=self.LEVEL3, kpi_name=kpi_name)

    def _get_direction_for_relative_position(self, value):
        """
        This function converts direction data from the template (as string) to a number.
        """
        if value == 'Y':
            value = 1000
        elif value in ('1', '1.0'):
            value = 1
        else:
            value = 0
        return value

    def write_to_db_result(self, kpi_set_fk, result, level, score=None, threshold=None, kpi_name=None, kpi_fk=None):
        """
        This function the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(kpi_set_fk, result=result, level=level, score=score,
                                                 threshold=threshold,
                                                 kpi_name=kpi_name, kpi_fk=kpi_fk)
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

    def create_attributes_dict(self, kpi_set_fk, result, level, score=None, threshold=None, kpi_name=None, kpi_fk=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = \
                self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].values[0]
            # attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
            #                             format(result, '.2f'), score_type, fk)],
            #                           columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
            #                                    'score_2', 'kpi_set_fk'])
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        result, kpi_set_fk,)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == kpi_fk]['kpi_name'].values[0].replace("'",
                                                                                                                    "\\'")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        kpi_fk, kpi_name, result)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            kpi_set_name = \
                self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].values[0]
            try:
                atomic_kpi_fk = \
                    self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == kpi_name]['atomic_kpi_fk'].values[0]
                kpi_fk = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == atomic_kpi_fk]['kpi_fk'].values[
                    0]
            except Exception as e:
                atomic_kpi_fk = None
                kpi_fk = None
            kpi_name = kpi_name.replace("'", "\\'")
            attributes = pd.DataFrame([(kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        result, kpi_fk, atomic_kpi_fk, threshold, score)],
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
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = self.rds_conn.db.cursor()
        delete_queries = PNGAMERICAQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = self.rds_conn.db.cursor()
        # for query in self.kpi_results_queries:
        #     try:
        #         cur.execute(query)
        #     except Exception as e:
        #         Log.info('Query {} failed due to {}'.format(query, e))
        #         continue
        queries = self.merge_insert_queries(self.kpi_results_queries)
        for query in queries:
            cur.execute(query)
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
