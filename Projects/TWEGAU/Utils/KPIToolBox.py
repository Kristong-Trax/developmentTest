import os
import re
from collections import defaultdict

import pandas as pd
from KPIUtils_v2.DB.CommonV2 import Common, PSProjectConnector

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nidhin'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
KPI_FAMILY = 'kpi_family_fk'
PS_KPI_FAMILY = 19
TYPE = 'type'
# ENTITY IDs
PRODUCT_ENTITY_ID = 1
BRAND_ENTITY_ID = 2
MANUFACTURER_ENTITY_ID = 3
CATEGORY_ENTITY_ID = 4
STORE_ENTITY_ID = 5
TEMPLATE_ENTITY_ID = 6
SUB_CATEGORY_ENTITY_ID = 7

EXCEL_ENTITY_MAP = {
    "product_fk": PRODUCT_ENTITY_ID,
    "brand_name": BRAND_ENTITY_ID,
    "manufacturer_name": MANUFACTURER_ENTITY_ID,
    "template_name": TEMPLATE_ENTITY_ID,
    "store": STORE_ENTITY_ID,
}

# Linear Based Template
LINEAR_KPI_SHEET = 'Linear KPI'
LINEAR_CATEGORY_SHEET = 'Linear Category'
# Zone Based Template
ZONE_KPI_SHEET = 'Zone Based KPI'
ZONE_CATEGORY_SHEET = 'Zone Based Category'
STORE_TYPE = 'store_type'
ZONE_NUMERATOR_FILTER_ENTITIES = ['filter_entity_1', 'filter_entity_2', 'filter_entity_3']
ZONE_DENOMINATOR_FILTER_ENTITIES = ['filter_entity_1', 'filter_entity_2', 'filter_entity_3']
SHELF_POLICY_FROM_TOP = 'shelf_policy_from_top'
NUMBER_OF_SHELVES = 'number_of_shelves'
SHELF_NUMBER = 'shelf_number'
FACING_SEQUENCE_NUMBER = 'facing_sequence_number'
ZONE_NAME = 'zone'
PROD_ID_COL_SCIF = "item_id"
# additional filters
# this is to filter but not group
# key - key name in self.scif
# value is list of tuples - (column name in sheet, actual value in DB)
ZONE_ADDITIONAL_FILTERS_PER_COL = {
    "template_name": [('template_name', 'template_name')],
}
# Column name map
KPI_TYPE = 'kpi_type'
KPI_NAME = 'kpi_name'
SCENE_FK = 'scene_fk'
STORE_FK = 'store_fk'
SCENE_TYPE_FK = "template_fk"
MANUFACTURER_FK = "manufacturer_fk"
NUMERATOR_FK = 'numerator_key'
DENOMINATOR_FK = 'denominator_key'
NUMERATOR_FILTER_ENTITIES = ['filter_entity_1', 'filter_entity_2', 'filter_entity_3']
DENOMINATOR_FILTER_ENTITIES = ['filter_entity_1', 'filter_entity_2']
EXCEL_DB_MAP = {
    "manufacturer_name": "manufacturer_fk",
    "template_name": "template_fk",
    "sub_category": "sub_category_fk",
    "brand_name": "brand_fk",
    "store": "store_id",
    "product_fk": "product_fk",
    'zone': 'context_type_fk',
    'bay_number': 'bay_number',
}
# additional filters
# this is to filter but not group
# key - key name in self.scif
# value is list of tuples - (column name in sheet, actual value in DB)
LINEAR_NUMERATOR_ADDITIONAL_FILTERS_PER_COL = {
    "brand_name": [('brand_others', 'Other'), ('brand_irrelevant', 'Irrelevant')],
    "template_name": [('template_name', 'template_name')],
}
# based on `stacking` column; select COL_FOR_MACRO_LINEAR_CALC
STACKING_COL = 'stacking'
STACKING_MAP = {0: 'gross_len_ign_stack', 1: 'gross_len_add_stack'}

ROUNDING_DIGITS = 4
exclude_re = re.compile("Excl( *)\((.*)\)")
only_re = re.compile("Only( *)\((.*)\)")


class TWEGAUToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.excel_file_path = os.path.join(self.templates_path, 'Template.xlsx')
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_name = self.store_info.store_name[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.empty_product_ids = self.all_products.query(
            'product_name.str.contains("empty", case=False) or'
            ' product_name.str.contains("irrelevant", case=False)',
            engine='python')['product_fk'].values

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # 1. calculate the macro linear sheet
        self.calculate_macro_linear()
        # 2. calculate the zone based sheet
        self.calculate_zone_based()
        self.common.commit_results_data()
        return 0

    def calculate_zone_based(self):
        zone_kpi_sheet = self.get_template_details(ZONE_KPI_SHEET)
        zone_category_sheet = self.get_template_details(ZONE_CATEGORY_SHEET)
        name_grouped_zone_kpi_sheet = zone_kpi_sheet.groupby(KPI_TYPE)
        for each_kpi in name_grouped_zone_kpi_sheet:
            each_kpi_type = each_kpi[0]
            kpi_sheet_rows = each_kpi[1]
            denominator_row = pd.Series()
            write_sku = False
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                       & (self.kpi_static_data[TYPE] == each_kpi_type)
                                       & (self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty:
                print("KPI Name:{} not found in DB".format(each_kpi_type))
            else:
                print("KPI Name:{} found in DB".format(each_kpi_type))
                if 'sku_all' in each_kpi_type.lower():
                    write_sku = True
                if 'sku_all' not in each_kpi_type.lower():
                    # there is no denominator for sku all
                    denominator_row = zone_category_sheet.loc[(
                            zone_category_sheet[KPI_NAME] == each_kpi_type)].iloc[0]
                list_of_zone_data = []
                for idx, each_kpi_sheet_row in kpi_sheet_rows.iterrows():
                    zone_data = self._get_zone_based_data(kpi, each_kpi_sheet_row.T,
                                                          denominator_row=denominator_row)
                    list_of_zone_data.append(zone_data)
                print "Final >>> ", list_of_zone_data
                if write_sku:
                    # write for products
                    _output_to_write = []
                    for each in list_of_zone_data:
                        if len(each) > 1:
                            _output_to_write.extend([x for x in each if 'unique_products' not in x])
                        else:
                            # No relevant product as per excel row config parameters
                            continue
                    if not _output_to_write:
                        continue
                    _df_output_to_write = pd.DataFrame(_output_to_write)
                    _df_output_to_write.dropna(subset=['bay_number'], inplace=True)
                    _grouped_output_to_write = _df_output_to_write.groupby(['scene_id', 'bay_number'], as_index=False)
                    for scene_bay_tuple, each_data_to_write in _grouped_output_to_write:
                        # remove empty products when getting all SKU's
                        products_to_write = set([prod_id for prod_list_per_bay in each_data_to_write["products"]
                                                 for prod_id in prod_list_per_bay])
                        data_to_write = each_data_to_write.iloc[0]
                        for each_product_id in products_to_write:
                            if int(each_product_id) not in self.empty_product_ids:
                                in_assort_sc = int(self.scif.query("item_id=={prod_id}"
                                                                   .format(prod_id=each_product_id))
                                                   .in_assort_sc.values[0])
                                self.common.write_to_db_result(
                                    fk=int(data_to_write.fk),
                                    numerator_id=int(each_product_id),
                                    numerator_result=int(data_to_write.bay_number),
                                    denominator_id=int(data_to_write.denominator_id),
                                    denominator_result=int(data_to_write.scene_id),
                                    result=int(data_to_write.zone_number),
                                    score=in_assort_sc,
                                    identifier_result=str(data_to_write.kpi_name),
                                    context_id=int(data_to_write.zone_number),
                                )
                else:
                    # its the calculation
                    _output_to_write = []
                    for each in list_of_zone_data:
                        if len(each) > 1:
                            _output_to_write.extend([x for x in each if 'unique_products' not in x])
                        else:
                            continue
                    if not _output_to_write:
                        continue
                    _df_output_to_write = pd.DataFrame(_output_to_write)
                    # remove rows with empty `products`
                    _df_output_to_write = _df_output_to_write[_df_output_to_write.astype(str)['products'] != "[]"]
                    _grouped_output_to_write = _df_output_to_write.groupby('denominator_id', as_index=False)
                    unique_manufacturer_products_count_data = _df_output_to_write.get(
                        "unique_manufacturer_products_count").values
                    if not len(unique_manufacturer_products_count_data):
                        continue
                    unique_manufacturer_products_count = unique_manufacturer_products_count_data[0]
                    for denominator_id, each_data_to_write in _grouped_output_to_write:
                        # remove empty products when getting all SKU's
                        # get all unique product ids from different dataframe rows in a set
                        products_to_write = set([prod_id for prod_list_per_bay in each_data_to_write["products"]
                                                 for prod_id in prod_list_per_bay])
                        # sanitize the products
                        products_to_write = [x for x in products_to_write if x not in self.empty_product_ids]
                        result = float(len(products_to_write)) / unique_manufacturer_products_count
                        data_to_write = each_data_to_write.iloc[0]  # for dot selection from first row
                        self.common.write_to_db_result(
                            fk=int(data_to_write.fk),
                            numerator_id=int(data_to_write.numerator_id),
                            denominator_id=int(data_to_write.denominator_id),
                            result=int(result),
                            score=1,
                            identifier_result=str(data_to_write.kpi_name),
                            context_id=int(data_to_write.zone_number),
                        )

    def calculate_macro_linear(self):
        kpi_sheet = self.get_template_details(LINEAR_KPI_SHEET)
        category_sheet = self.get_template_details(LINEAR_CATEGORY_SHEET)
        for index, kpi_sheet_row in kpi_sheet.iterrows():
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                       & (self.kpi_static_data[TYPE] == kpi_sheet_row[KPI_TYPE])
                                       & (self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty:
                print("KPI Name:{} not found in DB".format(kpi_sheet_row[KPI_NAME]))
            else:
                print("KPI Name:{} found in DB".format(kpi_sheet_row[KPI_NAME]))
                if not is_nan(kpi_sheet_row[STORE_TYPE]):
                    if bool(kpi_sheet_row[STORE_TYPE].strip()) and kpi_sheet_row[STORE_TYPE].strip().lower() != 'all':
                        print "Check the store types in excel..."
                        permitted_store_types = [x.strip() for x in kpi_sheet_row[STORE_TYPE].split(',') if x.strip()]
                        if self.store_info.store_type.values[0] not in permitted_store_types:
                            print "Store type not permitted..."
                            continue
                # get the length field
                length_field = STACKING_MAP[kpi_sheet_row[STACKING_COL]]
                # NUMERATOR
                numerator_filters, numerator_filter_string = get_filter_string_per_row(
                    kpi_sheet_row,
                    NUMERATOR_FILTER_ENTITIES,
                    additional_filters=LINEAR_NUMERATOR_ADDITIONAL_FILTERS_PER_COL)
                # Adding compulsory filtering values
                if numerator_filter_string:
                    numerator_filter_string += " and "
                # 1. remove empty/irrelevant products
                numerator_filter_string += "{prod_id_col_scif} not in {empty_prod_ids}".format(
                    prod_id_col_scif=PROD_ID_COL_SCIF,
                    empty_prod_ids=self.empty_product_ids.tolist()
                )
                # 2. remove zero facings
                numerator_filter_string += " and facings > 0"
                numerator_data = self.scif.query(numerator_filter_string).fillna(0). \
                    groupby(numerator_filters, as_index=False).agg({length_field: 'sum'})
                # DENOMINATOR
                denominator_row = category_sheet.loc[(category_sheet[KPI_NAME] == kpi_sheet_row[KPI_NAME])].iloc[0]
                denominator_filters, denominator_filter_string = get_filter_string_per_row(
                    denominator_row,
                    DENOMINATOR_FILTER_ENTITIES, )
                if denominator_filter_string:
                    # remove empty irrelevant products from denominator
                    # denominator_filter_string += "{prod_id_col_scif} not in {empty_prod_ids}".format(
                    #     prod_id_col_scif=PROD_ID_COL_SCIF,
                    #     empty_prod_ids=self.empty_product_ids.tolist()
                    # )
                    denominator_data = self.scif.query(denominator_filter_string).fillna(0). \
                        groupby(denominator_filters, as_index=False).agg({length_field: 'sum'})
                else:
                    # nothing to query; no grouping
                    # remove empty irrelevant products from denominator
                    # denominator_filter_string += "{prod_id_col_scif} not in {empty_prod_ids}".format(
                    #     prod_id_col_scif=PROD_ID_COL_SCIF,
                    #     empty_prod_ids=self.empty_product_ids.tolist()
                    # )
                    denominator_data = pd.DataFrame(self.scif.agg({length_field: 'sum'})).T
                for d_idx, denominator_row in denominator_data.iterrows():
                    denominator = denominator_row.get(length_field)
                    for idx, numerator_row in numerator_data.iterrows():
                        numerator = numerator_row.get(length_field)
                        try:
                            result = float(numerator) / float(denominator)
                        except ZeroDivisionError:
                            result = 0
                        numerator_id = int(numerator_row[EXCEL_DB_MAP[kpi_sheet_row.numerator_fk]])
                        denominator_key_str = EXCEL_DB_MAP[kpi_sheet_row.denominator_fk]
                        denominator_id = self.get_relavant_id(denominator_key_str,
                                                              numerator_row,
                                                              denominator_row)
                        context_id = None
                        if not is_nan(kpi_sheet_row.context_fk):
                            if bool(kpi_sheet_row.context_fk.strip()):
                                context_key_str = EXCEL_DB_MAP[kpi_sheet_row.context_fk]
                                context_id = self.get_relavant_id(context_key_str,
                                                                  numerator_row,
                                                                  denominator_row)

                        kpi_parent_name = None
                        should_enter = False
                        if not is_nan(kpi_sheet_row.kpi_parent_name):
                            kpi_parent_name = kpi_sheet_row.kpi_parent_name
                            should_enter = True
                        if not denominator_id:
                            raise Exception("Denominator ID cannot be found. [TWEGAU/Utils/KPIToolBox.py]")
                        self.common.write_to_db_result(fk=int(kpi['pk']),
                                                       numerator_id=numerator_id,
                                                       denominator_id=denominator_id,
                                                       context_id=context_id,
                                                       numerator_result=numerator,
                                                       denominator_result=denominator,
                                                       result=result,
                                                       score=result,
                                                       identifier_result=kpi_sheet_row[KPI_NAME],
                                                       identifier_parent=kpi_parent_name,
                                                       should_enter=should_enter,
                                                       )

    def get_shelf_limit_for_scene(self, scene_id):
        shelf_limit_per_scene_map = defaultdict(list)
        scene_data = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene_id]
        _bay_grouped_scene_data = scene_data.groupby('bay_number', as_index=False)
        for each_bay in _bay_grouped_scene_data:
            bay_number = each_bay[0]
            scene_data = each_bay[1]
            if not scene_data.empty:
                shelf_limit_per_scene_map[scene_id].append(
                    (
                        bay_number, {
                            'max_shelf': scene_data[SHELF_NUMBER].max(),
                            'min_shelf': scene_data[SHELF_NUMBER].min()
                        }
                    )
                )
        return shelf_limit_per_scene_map

    def get_valid_bay_numbers(self, scene_id, permitted_shelves):
        scene_max_min_map = self.get_shelf_limit_for_scene(scene_id)
        bay_numbers = []
        for scene_id, bay_shelf_map in scene_max_min_map.iteritems():
            for each_map in bay_shelf_map:
                _bay_number = each_map[0]
                scene_max_min_map = each_map[1]
                for each_permitted_shelf in permitted_shelves:
                    if scene_max_min_map['max_shelf'] == each_permitted_shelf:
                        bay_numbers.append(_bay_number)
        return bay_numbers

    def _get_zone_based_data(self, kpi, kpi_sheet_row, denominator_row):
        # generate scene max shelf max bay map
        zone_number = kpi_sheet_row[ZONE_NAME]
        shelves_policy_from_top = [int(x.strip()) for x in str(kpi_sheet_row[SHELF_POLICY_FROM_TOP]).split(',')
                                   if x.strip()]
        permitted_shelves = [int(x.strip()) for x in str(kpi_sheet_row[NUMBER_OF_SHELVES]).split(',') if
                             x.strip()]
        unique_manufacturer_products_count = 0
        # DENOMINATOR
        if not denominator_row.empty:
            denominator_filters, denominator_filter_string = get_filter_string_per_row(
                denominator_row,
                DENOMINATOR_FILTER_ENTITIES, )
            unique_manufacturer_products_count = len(self.scif.query(denominator_filter_string)
                                                     .product_fk.unique())
        # NUMERATOR
        # filter based on store type [d]
        # filter based on template_type [d]
        # find the shelf, and filter based on zone
        # filter based on exclude policy
        if not is_nan(kpi_sheet_row[STORE_TYPE]):
            if bool(kpi_sheet_row[STORE_TYPE].strip()) and kpi_sheet_row[STORE_TYPE].strip().lower() != 'all':
                print "Check the store types in excel..."
                permitted_store_types = [x.strip() for x in kpi_sheet_row[STORE_TYPE].split(',') if x.strip()]
                if self.store_info.store_type.values[0] not in permitted_store_types:
                    print "Store type not permitted..."
                    return []
        filters, filter_string = get_filter_string_per_row(
            kpi_sheet_row,
            ZONE_NUMERATOR_FILTER_ENTITIES,
            additional_filters=ZONE_ADDITIONAL_FILTERS_PER_COL,
        )
        # combined tables
        match_product_df = pd.merge(self.match_product_in_scene, self.products, how='left',
                                    left_on=['product_fk'], right_on=['product_fk'])

        scene_template_df = pd.merge(self.scene_info, self.templates, how='left',
                                     left_on=['template_fk'], right_on=['template_fk'])

        product_scene_join_data = pd.merge(match_product_df, scene_template_df, how='left',
                                           left_on=['scene_fk'], right_on=['scene_fk'])
        if filters:
            filtered_grouped_scene_items = product_scene_join_data.query(filter_string) \
                .groupby(filters, as_index=False)
        else:
            # dummy structure without filters
            filtered_grouped_scene_items = [('', product_scene_join_data.query(filter_string))]
        # get the scene_id's worth getting data from
        scene_data_map = []
        unique_products = set() # if the scene type not even valid
        for each_group_by_manf_templ in filtered_grouped_scene_items:
            # append scene to group by
            current_scene_types = each_group_by_manf_templ[1].template_name.unique()
            scene_type = current_scene_types
            if len(current_scene_types) == 1:
                scene_type = each_group_by_manf_templ[1].template_name.unique()[0]
            # unique_products = set()
            scene_type_grouped_by_scene = each_group_by_manf_templ[1].groupby(SCENE_FK)
            for each_scene_id_group in scene_type_grouped_by_scene:
                exclude_items = False
                include_only = False
                scene_id = each_scene_id_group[0]
                scene_data_per_scene = each_scene_id_group[1]
                valid_bay_numbers = self.get_valid_bay_numbers(scene_id, permitted_shelves)
                if not valid_bay_numbers:
                    continue
                matched_products_for_scene = scene_data_per_scene.query(
                    'shelf_number in {shelves} and bay_number in {bays}'.format(
                        shelves=shelves_policy_from_top,
                        bays=valid_bay_numbers
                    )).groupby(['bay_number', 'shelf_number'], as_index=False)
                # scene_uid = self.scene_info.loc[
                #     self.scene_info['scene_fk'] == each_scene].scene_uid.values[0]
                items_to_check_str = None
                if not is_nan(kpi_sheet_row.exclude_include_policy):
                    match_exclude = exclude_re.search(kpi_sheet_row.exclude_include_policy)
                    if not match_exclude:
                        match_only = only_re.search(kpi_sheet_row.exclude_include_policy)
                        items_to_check_str = match_only.groups()[-1]
                        include_only = True
                    else:
                        items_to_check_str = match_exclude.groups()[-1]
                        exclude_items = True

                for tuple_data in matched_products_for_scene:
                    # get excluding shelves
                    bay_number = tuple_data[0][0]
                    each_shelf_per_bay = tuple_data[1]
                    numerator_entity_id = EXCEL_ENTITY_MAP[kpi_sheet_row.numerator_fk]
                    numerator_id = getattr(each_shelf_per_bay,
                                           EXCEL_DB_MAP[kpi_sheet_row.numerator_fk],
                                           None).unique()[0]
                    denominator_entity_id = EXCEL_ENTITY_MAP[kpi_sheet_row.denominator_fk]
                    denominator_data = getattr(each_shelf_per_bay,
                                               EXCEL_DB_MAP[kpi_sheet_row.denominator_fk],
                                               pd.Series())
                    if denominator_data.empty:
                        # find in self
                        denominator_id = getattr(self, EXCEL_DB_MAP[kpi_sheet_row.denominator_fk], None)
                    else:
                        denominator_id = denominator_data.unique()[0]
                    if items_to_check_str:
                        # exclude/include logic
                        last_shelf_number = str(each_shelf_per_bay.n_shelf_items.unique()[0])
                        shelf_filter = items_to_check_str.replace('N', last_shelf_number)
                        shelf_filter_string = '[{}]'.format(shelf_filter)
                        if exclude_items:
                            # exclude rows in `items_to_check_tuple`
                            required_shelf_items = each_shelf_per_bay.drop(
                                each_shelf_per_bay.query('facing_sequence_number in {filter_string}'
                                                         .format(filter_string=shelf_filter_string)
                                                         ).index.tolist())
                        else:
                            # it is include_only:
                            required_shelf_items = each_shelf_per_bay.query(
                                'facing_sequence_number in {filter_string}'.format(
                                    filter_string=shelf_filter_string
                                ))
                        product_ids = required_shelf_items.product_fk.unique().tolist()
                    else:
                        product_ids = each_shelf_per_bay.product_fk.unique().tolist()
                    unique_products.update(product_ids)
                    scene_data_map.append(
                        {
                            'fk': int(kpi['pk']),
                            'numerator_id': numerator_id,
                            'denominator_id': denominator_id,
                            'bay_number': bay_number,
                            'scene_id': scene_id,
                            'products': product_ids,
                            'kpi_name': kpi_sheet_row[KPI_NAME],
                            'zone_number': zone_number,
                            'unique_manufacturer_products_count': unique_manufacturer_products_count,
                        }
                    )
        scene_data_map.append({"unique_products": unique_products})
        return scene_data_map

    def get_relavant_id(self, denominator_key_str, numerator_row, denominator_row):
        """

        :param denominator_key_str: str
        :param numerator_row: pd.Dataframe
        :param denominator_row: pd.Dataframe
        :return: int
                # zero check in self object
                # first check in denominator
                # second check in numerator
                # third check in self.scif
        >> always return one integer denominator_id
        """
        denominator_id = getattr(self, denominator_key_str, None)
        if not denominator_id:
            denominator_data = denominator_row.get(denominator_key_str,
                                                   numerator_row.get(denominator_key_str,
                                                                     self.scif.get(denominator_key_str)))
            if type(denominator_data) == pd.Series:
                denominator_id = denominator_data.drop_duplicates()[0]
            else:
                denominator_id = denominator_data
        return denominator_id

    def get_template_details(self, sheet_name):
        template = pd.read_excel(self.excel_file_path, sheetname=sheet_name)
        return template


def is_int(value):
    try:
        int(value)
    except ValueError:
        return False
    else:
        return True


def is_nan(value):
    if value != value:
        return True
    return False


def get_filter_string_per_row(kpi_sheet_row, filter_entities, additional_filters=None):
    """
    Function to generate the filter string with list of filters.
    :param kpi_sheet_row: pd.Series
    :param filter_entities: list of filters as in excel
    :param additional_filters: None or dictionary with tuple values
    :return: tuple >> (filters, filter_string)
    """
    filter_string = ''
    filters = []
    if not additional_filters:
        additional_filters = {}
    # generate the numerator filter string
    for each_filter in filter_entities:
        filter_key = kpi_sheet_row[each_filter]
        if is_nan(filter_key):
            # it is NaN ~ it is empty
            continue
        # grab the filters anyways to group
        filters.append(EXCEL_DB_MAP[filter_key])
        filter_value = kpi_sheet_row[each_filter + '_value']
        if filter_value.lower() == "all":
            # ignore the filter
            continue
        filter_data = str([x.strip() for x in filter_value.split(',') if x.strip()])
        filter_string += "{key} in {data_list} and ".format(
            key=filter_key,
            data_list=filter_data
        )
    # add additional filters , all joined with `and`
    for col_name, filter_tuples in additional_filters.iteritems():
        temp_add_filter_string = ''
        for each_filter in filter_tuples:
            if is_nan(kpi_sheet_row[each_filter[0]]) or \
                    kpi_sheet_row[each_filter[0]] == "NA":
                # it is NaN(empty) or NA
                continue
            elif not is_int(kpi_sheet_row[each_filter[0]]):
                # its a string -- check == with values from sheet
                filter_data = str([x.strip() for x in kpi_sheet_row[each_filter[0]].split(',') if x.strip()])
                filter_string += "{key} in {data_list} ".format(
                    key=col_name,
                    data_list=filter_data
                )
            elif not int(kpi_sheet_row[each_filter[0]]):
                # it is integer -- detect only the false's
                temp_add_filter_string += '{key}!="{value}" or '. \
                    format(key=col_name, value=each_filter[1].strip())
            temp_add_filter_string = temp_add_filter_string.rstrip('or ')
        if not filter_string.endswith(" and "):
            filter_string += temp_add_filter_string + " and "
    filter_string = filter_string.rstrip('and ')
    return filters, filter_string
