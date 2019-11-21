import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from Trax.Utils.Logging.Logger import Log

__author__ = 'nidhinb'
# own check
OWN_CHECK_COL = 'att1'  # !column in product dataframe!
OWN_DISTRIBUTOR_FK = 60  # SinoPac distributor ID in custom entity table as well as manufacturer ID.
OTHER_DISTRIBUTOR_FK = 2  # Non SinoPac distributor ID in the custom entity table.
OWN_DISTRIBUTOR = 'SINO PACIFIC'

TEMPLATE_PARENT_FOLDER = 'Data'
TEMPLATE_NAME = 'Template.xlsx'

KPI_NAMES_SHEET = 'kpis'
ASSORTMENT_SHEET = 'assortment'
KPI_DETAILS_SHEET = 'details'
KPI_INC_EXC_SHEET = 'exclude_include'
# Column Name
KPI_NAME_COL = 'kpi_name'
KPI_ACTIVE = 'active'
KPI_PARENT_COL = 'kpi_parent'
KPI_TYPE_COL = 'type'
KPI_FAMILY_COL = 'kpi_family'
STORE_POLICY = 'store_policy'
OUTPUT_TYPE = 'output'
# FAMILIES ALLOWED
ASSORTMENTS = 'ASSORTMENTS'
FSOS = 'FSOS'
SIMON ='SIMON'
OOS = 'OOS'
Count = 'Count'
# Output Type
NUM_OUT = 'number'
LIST_OUT = 'list'
PARAM_COUNT = 4
PARAM_DB_MAP = {
    # key: the value in excel as param values
    # value:
    #       key - to be checked in DataBase
    #       name - name as in DataBase
    'brand': {'key': 'brand_fk', 'name': 'brand_name'},
    'product': {'key': 'product_fk', 'name': 'product_name'},
    'category': {'key': 'category_fk', 'name': 'category'},
    'scene_type': {'key': 'template_fk', 'name': 'template_name'},
    'sub_category': {'key': 'sub_category_fk', 'name': 'sub_category'},
    'manufacturer': {'key': 'manufacturer_fk', 'name': 'manufacturer_name'},
    'distributor': {'key': OWN_CHECK_COL, 'name': OWN_CHECK_COL},
    'store': {'key': 'store_fk', 'name': 'store_name'},
}
# list of `exclude_include` sheet columns
INC_EXC_LIST = ['stacking', 'others', 'irrelevant', 'empty',
                'categories_to_include', 'scene_types_to_include',
                'brands_to_include', 'ean_codes_to_include']
# assortment KPIs
# Codes
OOS_CODE = 1
PRESENT_CODE = 2
EXTRA_CODE = 3
# Assortment KPI Names
DST_MAN_BY_STORE_PERC = 'DST_DISTRIBUTOR_BY_STORE_PERC'
OOS_MAN_BY_STORE_PERC = 'OOS_DISTRIBUTOR_BY_STORE_PERC'
PRODUCT_PRESENCE_BY_STORE_LIST = 'DST_DISTRIBUTOR_BY_STORE_PERC - SKU'
OOS_PRODUCT_BY_STORE_LIST = 'OOS_DISTRIBUTOR_BY_STORE_PERC - SKU'
#  # Category based Assortment KPIs
DST_MAN_BY_CATEGORY_PERC = 'DST_DISTRIBUTOR_ALL_CATEGORY'
OOS_MAN_BY_CATEGORY_PERC = 'OOS_DISTRIBUTOR_ALL_CATEGORY'
PRODUCT_PRESENCE_BY_CATEGORY_LIST = 'DST_DISTRIBUTOR_ALL_CATEGORY - SKU'
OOS_MAN_BY_CATEGORY_LIST = 'OOS_DISTRIBUTOR_ALL_CATEGORY - SKU'

# map to save list kpis
CODE_KPI_MAP = {
    OOS_CODE: OOS_PRODUCT_BY_STORE_LIST,
    PRESENT_CODE: PRODUCT_PRESENCE_BY_STORE_LIST,
}
# policy JSON map: key is what is in the policy and value corresponds to the one present in the self.store_info below
POLICY_STORE_MAP = {
    'retailer': 'retailer_name',
    'region': 'region_name',
}


def _sanitize_csv(data):
    if type(data) == list:
        # not handling ["val1", "val2,val3", "val4"]
        return [x.strip() for x in data if x.strip()]
    else:
        return [each.strip() for each in data.split(',') if each.strip()]


class SINOTHSceneToolBox:

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider.scene_item_facts
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.current_scene_fk = self.scene_info.iloc[0].scene_fk
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.store_info.iloc[0].store_fk
        self.store_type = self.data_provider.store_type
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.targets = self.ps_data_provider.get_kpi_external_targets()
        self.match_display_in_scene = self.data_provider.match_display_in_scene
        self.scene_template_info = self.scif[['scene_fk',
                                              'template_fk', 'template_name']].drop_duplicates()
        self.kpi_template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                              '..', TEMPLATE_PARENT_FOLDER,
                                              TEMPLATE_NAME)
        self.kpi_template = pd.ExcelFile(self.kpi_template_path)
        self.own_man_fk = OWN_DISTRIBUTOR_FK
        self.kpi_template = pd.ExcelFile(self.kpi_template_path)
        self.empty_prod_ids = self.all_products[
            self.all_products.product_name.str.contains('empty', case=False)]['product_fk'].values
        self.irrelevant_prod_ids = self.all_products[
            self.all_products.product_name.str.contains('irrelevant', case=False)]['product_fk'].values
        self.other_prod_ids = self.all_products[
            self.all_products.product_name.str.contains('other', case=False)]['product_fk'].values

    def filter_and_send_kpi_to_calc(self):
        kpi_sheet = self.kpi_template.parse(KPI_NAMES_SHEET)
        kpi_sheet[KPI_FAMILY_COL] = kpi_sheet[KPI_FAMILY_COL].fillna(method='ffill')
        kpi_details = self.kpi_template.parse(KPI_DETAILS_SHEET)
        kpi_include_exclude = self.kpi_template.parse(KPI_INC_EXC_SHEET)
        for index, kpi_sheet_row in kpi_sheet.iterrows():
            if not is_nan(kpi_sheet_row[KPI_ACTIVE]):
                if str(kpi_sheet_row[KPI_ACTIVE]).strip().lower() in ['0.0', 'n', 'no']:
                    Log.warning("KPI :{} deactivated in sheet.".format(kpi_sheet_row[KPI_NAME_COL]))
                    continue
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == kpi_sheet_row[KPI_NAME_COL])
                                       & (self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty or kpi.session_relevance.values[0] == 1:
                Log.info(
                    "*** KPI Name:{name} not found in DB or is a SESSION LEVEL KPI for scene {scene} ***".format(
                        name=kpi_sheet_row[KPI_NAME_COL],
                        scene= self.current_scene_fk
                    ))
                continue
            else:
                Log.info("KPI Name:{name} found in DB for scene {scene}".format(
                    name=kpi_sheet_row[KPI_NAME_COL],
                    scene= self.current_scene_fk
                ))
                detail = kpi_details[kpi_details[KPI_NAME_COL] == kpi[KPI_TYPE_COL].values[0]]
                # check for store types allowed
                permitted_store_types = [x.strip().lower()
                                         for x in detail[STORE_POLICY].values[0].split(',') if x.strip()]
                if self.store_info.store_type.iloc[0].lower() not in permitted_store_types:
                    if permitted_store_types and permitted_store_types[0] != "all":
                        Log.warning("Not permitted store type - {type} for scene {scene}".format(
                            type=kpi_sheet_row[KPI_NAME_COL],
                            scene= self.current_scene_fk
                        ))
                        continue
                detail['pk'] = kpi['pk'].iloc[0]
                # gather details
                groupers, query_string = get_groupers_and_query_string(detail)
                kpi_include_exclude = kpi_include_exclude[kpi_include_exclude.kpi_name != ASSORTMENTS]
                _include_exclude = kpi_include_exclude[kpi_details[KPI_NAME_COL]
                                                       == kpi[KPI_TYPE_COL].values[0]]
                # gather include exclude
                include_exclude_data_dict = get_include_exclude(_include_exclude)
                dataframe_to_process = self.get_sanitized_match_prod_scene(
                    include_exclude_data_dict)
            # hack to cast all other than OWN_DISTRIBUTOR to non-sino
            non_sino_index = dataframe_to_process[OWN_CHECK_COL] != OWN_DISTRIBUTOR
            dataframe_to_process.loc[non_sino_index, OWN_CHECK_COL] = 'non-sino'
            if kpi_sheet_row[KPI_FAMILY_COL] in [FSOS, SIMON]:
                self.calculate_fsos(detail, groupers, query_string, dataframe_to_process)
            else:
                Log.error("From project: {proj}. Unexpected kpi_family: {type}. Please check.".format(
                    type=kpi_sheet_row[KPI_FAMILY_COL],
                    proj=self.project_name
                ))
                pass
        return True

    def calculate_fsos(self, kpi, groupers, query_string, dataframe_to_process):
        Log.info("Calculate {name} for scene {scene}".format(
            name=kpi.kpi_name.iloc[0],
            scene=self.current_scene_fk
        ))
        if query_string:
            grouped_data_frame = dataframe_to_process.query(query_string).groupby(groupers)
        else:
            grouped_data_frame = dataframe_to_process.groupby(groupers)
        for group_id_tup, group_data in grouped_data_frame:
            if type(group_id_tup) not in [tuple, list]:
                # convert to a tuple
                group_id_tup = group_id_tup,
            param_id_map = dict(zip(groupers, group_id_tup))
            # the hack! This casts the value of distributor to that in DB as custom entity.
            if OWN_CHECK_COL in param_id_map:
                distributor_name = param_id_map.pop(OWN_CHECK_COL)
                if distributor_name == OWN_DISTRIBUTOR:
                    param_id_map[OWN_CHECK_COL] = self.own_man_fk  # as per custom entity
                else:
                    param_id_map[OWN_CHECK_COL] = OTHER_DISTRIBUTOR_FK  # as per custom entity
                param_id_map['manufacturer_fk'] = param_id_map[OWN_CHECK_COL]
            # SET THE numerator, denominator and context
            numerator_id = param_id_map.get(PARAM_DB_MAP[kpi['numerator'].iloc[0]]['key'])
            if numerator_id is None:
                raise Exception("Numerator cannot be null. Check SinoTH KPIToolBox [calculate_fsos].")
            denominator_id = get_parameter_id(key_value=PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'],
                                              param_id_map=param_id_map)
            if denominator_id is None:
                # because 0 is good; check None specifically
                denominator_id = self.store_id
            context_id = get_parameter_id(key_value=PARAM_DB_MAP[kpi['context'].iloc[0]]['key'],
                                          param_id_map=param_id_map)
            if context_id is None:
                # because 0 is good
                context_id = self.store_id
            if PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'] == 'store_fk':
                denominator_df = dataframe_to_process
            else:
                denominator_df = dataframe_to_process.query('{key} == {value}'.format(
                    key=PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'],
                    value=denominator_id))
            if not len(denominator_df):
                Log.error("No denominator data for session {sess} and scene {scene} to calculate  {name}".format(
                    sess=self.session_uid,
                    name=kpi.kpi_name.iloc[0],
                    scene=self.current_scene_fk
                ))
                raise Exception("Denominator data cannot be null. Check SinoTH KPIToolBox [calculate_fsos].")
            result = len(group_data) / float(len(denominator_df))
            # its the parent. Save the identifier result.
            self.common.write_to_db_result(fk=kpi['pk'].iloc[0],
                                           numerator_id=numerator_id,
                                           denominator_id=denominator_id,
                                           context_id=context_id,
                                           result=result,
                                           numerator_result=len(group_data),
                                           denominator_result=len(denominator_df),
                                           identifier_result="{}_{}_{}_{}".format(
                                               kpi['kpi_name'].iloc[0],
                                               kpi['pk'].iloc[0],
                                               # numerator_id,
                                               denominator_id,
                                               context_id,
                                           ),
                                           should_enter=True,
                                           by_scene=True,
                                           )

        return True

    def get_sanitized_match_prod_scene(self, include_exclude_data_dict):
        scene_product_data = self.match_product_in_scene.merge(
            self.products, how='left', on=['product_fk'], suffixes=('', '_prod')
        )
        sanitized_products_in_scene = scene_product_data.merge(
            self.scene_template_info, how='left', on='scene_fk', suffixes=('', '_scene')
        )
        # flags
        include_empty = include_exclude_data_dict.get('empty')
        include_irrelevant = include_exclude_data_dict.get('irrelevant')
        include_others = include_exclude_data_dict.get('others')
        include_stacking = include_exclude_data_dict.get('stacking')
        # list
        scene_types_to_include = include_exclude_data_dict.get('scene_types_to_include', False)
        categories_to_include = include_exclude_data_dict.get('categories_to_include', False)
        brands_to_include = include_exclude_data_dict.get('brands_to_include', False)
        ean_codes_to_include = include_exclude_data_dict.get('ean_codes_to_include', False)
        # Start include items
        if scene_types_to_include:
            # list of scene types to include is present, otherwise all included
            Log.info("Include template/scene type {}".format(scene_types_to_include))
            sanitized_products_in_scene = sanitized_products_in_scene[
                sanitized_products_in_scene['template_name'].str.upper().isin(
                    [x.upper() if type(x) in [unicode, str] else x for x in scene_types_to_include]
                )]
        if not include_stacking:
            # exclude stacking if the flag is set
            Log.info("Exclude stacking other than in layer 1 or negative stacking [menu]")
            sanitized_products_in_scene = sanitized_products_in_scene.loc[
                sanitized_products_in_scene['stacking_layer'] <= 1]
        if categories_to_include:
            # list of categories to include is present, otherwise all included
            Log.info("Include categories {}".format(categories_to_include))
            sanitized_products_in_scene = sanitized_products_in_scene[
                sanitized_products_in_scene['category'].str.upper().isin(
                    [x.upper() if type(x) in [unicode, str] else x for x in categories_to_include]
                )]
        if brands_to_include:
            # list of brands to include is present, otherwise all included
            Log.info("Include brands {}".format(brands_to_include))
            sanitized_products_in_scene = sanitized_products_in_scene[
                sanitized_products_in_scene['brand_name'].str.upper().isin(
                    [x.upper() if type(x) in [unicode, str] else x for x in brands_to_include]
                )]
        if ean_codes_to_include:
            # list of ean_codes to include is present, otherwise all included
            Log.info("Include ean codes {}".format(ean_codes_to_include))
            sanitized_products_in_scene = sanitized_products_in_scene[
                sanitized_products_in_scene['product_ean_code'].str.upper().isin(
                    [x.upper() if type(x) in [unicode, str] else x for x in ean_codes_to_include]
                )]
        product_ids_to_exclude = []
        if not include_irrelevant:
            # add product ids to exclude with irrelevant
            product_ids_to_exclude.extend(self.irrelevant_prod_ids)
        if not include_others:
            # add product ids to exclude with others
            product_ids_to_exclude.extend(self.other_prod_ids)
        if not include_empty:
            # add product ids to exclude with empty
            product_ids_to_exclude.extend(self.empty_prod_ids)
        if product_ids_to_exclude:
            Log.info("Exclude product ids {}".format(product_ids_to_exclude))
            sanitized_products_in_scene.drop(
                sanitized_products_in_scene[
                    sanitized_products_in_scene['product_fk'].isin(product_ids_to_exclude)].index,
                inplace=True
            )
        return sanitized_products_in_scene


def get_parameter_id(key_value, param_id_map):
    """Function to return parameter ID.

    Return the context ID from the dict provided based on the numerator/denominator in excel. Or return `None`.
    Use appropriate default ID in caller; if context ID cannot be found. Usually store_id.
    """
    if key_value.strip() and key_value.strip() != 'store_fk':
        return param_id_map.get(key_value, None)
    return None


def get_include_exclude(kpi_include_exclude):
    output = {}
    for key in INC_EXC_LIST:
        if not is_nan(kpi_include_exclude.get(key).values[0]) and \
                str(kpi_include_exclude.get(key).values[0]).strip() != '' and \
                str(kpi_include_exclude.get(key).values[0]).strip().lower() not in ['na', 'n/a', 'n / a']:
            _data = kpi_include_exclude.get(key).values[0]
            if type(_data) == unicode:
                _data = [x.strip() for x in _data.split(',') if x.strip()]
            output[key] = _data
    return output


def get_groupers_and_query_string(detail):
    keys = ['param_{}'.format(x) for x in range(1, PARAM_COUNT + 1)]
    filters = []
    filter_string = ''
    for each_key in keys:
        if not is_nan(detail[each_key].iloc[0]):
            filters.append(PARAM_DB_MAP[detail[each_key].iloc[0]]['key'])
            if not is_nan(detail[each_key + "_value"].iloc[0]):
                filter_data = [str(x.strip()) for x in detail[each_key +
                                                              "_value"].iloc[0].split(',') if x.strip()]
                filter_string += ' {key} in {data_list} and'.format(
                    key=PARAM_DB_MAP[detail[each_key].iloc[0]]['name'],
                    data_list=filter_data
                )
    return filters, filter_string.strip('and')


def is_nan(value):
    if value != value:
        return True
    return False
