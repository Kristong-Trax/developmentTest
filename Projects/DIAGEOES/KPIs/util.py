from KPIUtils.Calculations.Assortment import Assortment
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
from OutOfTheBox.Calculations.SOSBase import BaseFieldRetriever
from Trax.Algo.Calculations.Core.KPI.UnifiedKpiSingleton import UnifiedKPISingleton
from KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil import TemplateHandler
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils.DB.Common import Common as OldCommon
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Consts.DataProvider import MatchesConsts, ProductsConsts
from KPIUtils_v2.Utils.Consts.GlobalConsts import BasicConsts
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider as PsDataProvider_v2
from KPIUtils_v2.Calculations.MenuCalculations import Menu
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
import pandas as pd
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider as PsDataProvider_v1
from Trax.Algo.Calculations.Core.DataProvider import Output
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Keys import DbUsers


class DiageoUtil(UnifiedKPISingleton):

    def __init__(self, data_provider, output=None, menu=False, front_facing=False):
        super(DiageoUtil, self).__init__(data_provider)
        self.data_provider = data_provider
        self.commonV2 = Common(data_provider)
        self.common = OldCommon(data_provider)
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.data_provider.project_name, DbUsers.CalculationEng)
        self.diageo_generator = DIAGEOGenerator(self.data_provider, output, self.commonV2)
        self.diageo_manufacturer = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.template_handler = TemplateHandler(self.data_provider.project_name)
        self.template_handler.update_templates()
        self.relative_position_template = self.template_handler.download_template(DiageoKpiNames.RELATIVE_POSITION)
        self.assortment = Assortment(self.data_provider, output)
        self.assortment_lvl3_results = self.assortment.calculate_lvl3_assortment()
        self.Log = Log
        self.templates = self.data_provider[Data.ALL_TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.ps_data_provider_v2 = PsDataProvider_v2(self.data_provider, output)
        self.sub_brands = self._get_sub_brands()
        self.result_values = self.ps_data_provider_v2.get_result_values()
        self.store_id = self.data_provider[Data.STORE_FK]
        self.own_manuf_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.menu = Menu(data_provider=self.data_provider) if menu else None
        self.scenes_info = data_provider[Data.SCENES_INFO]
        self.output = Output
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.ps_data_provider = PsDataProvider_v1(self.data_provider, self.output)
        self.store_info = self.ps_data_provider.get_ps_store_info(self.store_info)
        self.store_channel = self.store_info['store_type'].values[0]
        if self.store_channel:
            self.store_channel = self.store_channel.upper()
        self.front_facing = front_facing
        if self.front_facing:
            self.scif = self.scif[self.scif['front_face_count'] == 1]

    @property
    def assortment_lvl2_results(self):
        if self.assortment_lvl3_results:
            return self.assortment.calculate_lvl2_assortment(self.assortment_lvl3_results)
        return None

    def get_template_data(self, kpi_name):
        template_data = self.template_handler.download_template(kpi_name)
        return template_data

    def get_product_fk_by_ean_code(self, ean_code):
        """This method returns the matching product_fk to the ean_code"""
        product_fk = self.all_products.loc[self.all_products.product_ean_code == ean_code, MatchesConsts.PRODUCT_FK]
        if product_fk.empty:
            self.Log.warning(DiageoConsts.WRONG_EAN_CODE_LOG.format(ean_code))
            return -1
        return product_fk.iloc[0]

    def get_numerator_id_by_entity(self, entity_type, entity_value):
        """
        This method retrieve the relevant entity_fk according to it's type.
        Currently the entity_type will be product_ean_code but this is in case they will decide to extend this KPI.
        :param entity_type: The relevant entity. E.g: product_ean_code
        :param entity_value: The value from the template.
        :return: The relevant entity fk.
        """
        if entity_type == ProductsConsts.PRODUCT_EAN_CODE:
            product_fk = self.get_product_fk_by_ean_code(entity_value)
            return product_fk
        else:
            self.Log.warning(DiageoConsts.MISSING_ENTITY_IN_TEMPLATE_LOG.format(entity_type))
            return -1

    def _get_params_types(self, params):
        """
        this method finds the type of tested and anchor params
        :param params: params for filtering
        :return: scif_anchor_param, scif_tested_param
        """
        scif_tested_param = self._get_type(params, DiageoConsts.TESTED_TYPE)
        scif_anchor_param = self._get_type(params, DiageoConsts.ANCHOR_TYPE)
        return scif_anchor_param, scif_tested_param

    @staticmethod
    def _get_type(params, scif_type):
        """
        helper method for _get_params_types
        :param params: params for filtering
        :param scif_type:
        :return:
        """
        if params.get(scif_type, '') == DiageoConsts.BRAND:
            scif_param = 'brand_name'
        elif params.get(scif_type, '') == DiageoConsts.SUB_BRAND_NAME_RP:
            scif_param = 'sub_brand_name'
        else:
            scif_param = ProductsConsts.PRODUCT_EAN_CODE
        return scif_param

    @staticmethod
    def _get_direction_for_relative_position(value):
        """
        This function converts direction data from the template (as string) to a number.
        """
        if value == DiageoConsts.UNLIMITED_DISTANCE:
            value = 1000
        elif not value or not str(value).isdigit():
            value = 0
        else:
            value = int(value)
        return value

    @staticmethod
    def _get_relative_position_kpi_name(tested, anchor):
        """
        this method finds the correct kpi name according to tested and anchor types
        :param tested: tested type
        :param anchor: anchor type
        :return: kpi name
        """
        if tested == anchor == ProductsConsts.PRODUCT_EAN_CODE:
            return DiageoConsts.RELATIVE_POSITION_PRODUCT_TO_PRODUCT
        elif tested == anchor == ProductsConsts.BRAND_NAME:
            return DiageoConsts.RELATIVE_POSITION_BRAND_TO_BRAND
        elif tested == anchor == DiageoConsts.SUB_BRAND_NAME:
            return DiageoConsts.RELATIVE_POSITION_SUB_BRAND_TO_SUB_BRAND
        elif tested == DiageoConsts.SUB_BRAND_NAME and anchor == ProductsConsts.BRAND_NAME:
            return DiageoConsts.RELATIVE_POSITION_SUB_BRAND_TO_BRAND
        else:
            return DiageoConsts.RELATIVE_POSITION_PRODUCT_TO_BRAND

    def _get_entities_ids(self, scif_anchor_param, scif_tested_param, params):
        """
        This method extracts the ids of tested and anchor entity (product, brand, sub_brand)
        :param scif_tested_param: The type of the tested param
        :param params: User's template specific position
        :param scif_anchor_param: The type of the anchor param
        :return: Tuple which includes the anchor_id and the tested_id
        """
        # Handling the tested entity
        if scif_tested_param == ProductsConsts.PRODUCT_EAN_CODE:
            tested_id = self.all_products.loc[
                self.all_products[ProductsConsts.PRODUCT_EAN_CODE] == params.get(DiageoConsts.TESTED), ProductsConsts.PRODUCT_FK]
        elif scif_tested_param == ProductsConsts.BRAND_NAME:
            tested_id = self.all_products.loc[
                self.all_products[ProductsConsts.BRAND_NAME] == params.get(DiageoConsts.TESTED_PRODUCT_NAME), ProductsConsts.BRAND_FK]
        elif scif_tested_param == DiageoConsts.SUB_BRAND_NAME:
            tested_id = self.sub_brands.loc[
                self.sub_brands['name'] == params.get(DiageoConsts.TESTED_PRODUCT_NAME), BasicConsts.PK]
        else:
            Log.error("Couldn't retrieve PK for tested entity: {}".format(scif_tested_param))
            return 0, 0
        # Handling the anchor entity
        if scif_anchor_param == ProductsConsts.PRODUCT_EAN_CODE:
            anchor_id = self.all_products.loc[
                self.all_products[ProductsConsts.PRODUCT_EAN_CODE] == params.get(DiageoConsts.ANCHOR),
                ProductsConsts.PRODUCT_FK]
        elif scif_anchor_param == ProductsConsts.BRAND_NAME:
            anchor_id = self.all_products.loc[
                self.all_products[ProductsConsts.BRAND_NAME] == params.get(DiageoConsts.ANCHOR_PRODUCT_NAME),
                ProductsConsts.BRAND_FK]
        elif scif_anchor_param == DiageoConsts.SUB_BRAND_NAME:
            anchor_id = self.sub_brands.loc[
                self.sub_brands['name'] == params.get(DiageoConsts.ANCHOR_PRODUCT_NAME), BasicConsts.PK]
        else:
            Log.warning("Couldn't retrieve PK for the anchor entity: {}".format(scif_anchor_param))
            return 0, 0
        try:
            return tested_id.values[0], anchor_id.values[0]
        except IndexError:
            Log.error(
                "One of the following don't exist in self.all_products: {} of {}"
                " or {} of {}".format(scif_tested_param, DiageoConsts.TESTED_PRODUCT_NAME, scif_anchor_param,
                                      DiageoConsts.ANCHOR_PRODUCT_NAME))
            return 0, 0

    def get_sub_brand(self):
        query = DiageoConsts.SUB_BRAND_QUERY
        sub_brands = pd.read_sql_query(query, self.rds_conn.db)
        return sub_brands

    def _get_sub_brands(self):
        """
        This method responsible to fetch the sub brands from the DB.
        :return: A DataFrame with the sub_brand_name, entity_fk and brand_fk.
        """
        kpi_entity_type = self.ps_data_provider_v2.get_kpi_entity_types()
        try:
            sub_brand_entity_fk = kpi_entity_type.loc[kpi_entity_type.name == DiageoConsts.SUB_BRAND]['pk'].iloc[0]
            sub_brand_df = self.ps_data_provider_v2.get_custom_entities(sub_brand_entity_fk)
            sub_brand_df.rename({'parent_id': 'brand_fk'}, axis='columns', inplace=True)
        except IndexError:
            Log.debug(DiageoConsts.MISSING_SUB_BRAND_ENTITY)
            sub_brand_df = self.get_sub_brand()
        return sub_brand_df

    def _get_template_fk_by_scene_type(self, scene_type):
        """
        :param scene_type: this needs to be an instance of template_name from scif (or a derivative)
        :return: template_fk
        """
        filtered_scif = self.templates[self.templates['template_name'].str.encode('utf8') == scene_type.encode('utf8')]
        if filtered_scif.empty:
            Log.debug(DiageoConsts.WRONG_TEMPLATE_NAME_IN_TEMPLATE_LOG.format(scene_type))
            return None
        return filtered_scif.template_fk.values[0]

    def _write_results_to_old_tables(self, kpi_name, score, level=1):
        """
        This method responsible to write the results to the old tables.
        The atomic and the kpi level will have the same scores in our case.
        """
        if level == 1:
            try:
                set_fk = self.commonV2.get_kpi_fk_by_kpi_name(kpi_name, 1)
                self.commonV2.write_to_db_result(fk=set_fk, level=level, score=score)
            except IndexError:
                Log.error("Couldn't find the following set_name in the old kpi tables: {}".format(kpi_name))
        else:
            try:
                kpi_fk = self.commonV2.get_kpi_fk_by_kpi_name(kpi_name, 2)
                atomic_fk = self.commonV2.get_kpi_fk_by_kpi_name(kpi_name, 3)
                self.commonV2.write_to_db_result(fk=kpi_fk, level=2, score=score)
                self.commonV2.write_to_db_result(fk=atomic_fk, level=3, score=score)
            except IndexError:
                Log.error("Couldn't find the following kpi_name in the old kpi tables: {}".format(kpi_name))

    def _get_relevant_template_fk(self, params):
        """
        This method gets the KPI params and returns the relevant template_fk if exists, else, returns None.
        """
        template_fk = None
        if DiageoConsts.LOCATION in params.keys():
            template_fk = self._get_template_fk_by_scene_type(params[DiageoConsts.LOCATION])
        return template_fk


    def get_kpi_fk_by_entity_type(self, entity_type):
        """
        This function returns the relevant kpi according to the entity.
        Meanwhile there is only product_ean_codes but in the future there is possibility to add more.
        :param entity_type: The relevant entity. E.g: product_ean_code
        :return: The relevant kpi_fk
        """
        kpi_fk = 0
        if entity_type == ProductsConsts.PRODUCT_EAN_CODE:
            kpi_fk = self.commonV2.get_kpi_fk_by_kpi_type(DiageoConsts.SHELF_PLACEMENT_BY_SKU_KPI)
        else:
            self.Log.warning(DiageoConsts.UNSUPPORTED_ENTITY_LOG.format(entity_type))
        return kpi_fk

    def get_result_value_util(self, value_to_fetch):
        """This is a util function for _get_results_type_and_value_entities that fetches the relevant pk per value from
        static.kpi_result_value"""
        entity_fk = self.result_values.loc[self.result_values.value == str(value_to_fetch), 'pk']
        if entity_fk.empty:
            Log.warning(DiageoConsts.MISSING_KPI_RESULT_VALUE_LOG.format(value_to_fetch))
            return 0
        return entity_fk.iloc[0]

    def get_results_value_entities(self, location, target_group):
        """
        This method fetches the relevant entity_fks from the table static.kpi_result_value in order to present
        the requested results ('E', 'T' etc) in the mobile report.
        :return: location fk and target fk from kpi_result_value table
        """
        location_fk = self.get_result_value_util(location)
        target_group = ', '.join(target_group)      # Convert it from list to string
        target_group_fk = self.get_result_value_util(target_group)
        return location_fk, target_group_fk

    def build_dictionary_for_db_insert_v2(self, fk=None, kpi_name=None, numerator_id=0, numerator_result=0, result=0,
                                          denominator_id=0, denominator_result=0, score=0,
                                          denominator_result_after_actions=None, context_id=None, target=None,
                                          identifier_parent=None, identifier_result=None, should_enter=None):
        try:
            insert_params = dict()
            if not fk:
                if not kpi_name:
                    return
                else:
                    insert_params['fk'] = self.commonV2.get_kpi_fk_by_kpi_name(kpi_name)
            else:
                insert_params['fk'] = fk
            insert_params['numerator_id'] = numerator_id
            insert_params['numerator_result'] = numerator_result
            insert_params['denominator_id'] = denominator_id
            insert_params['denominator_result'] = denominator_result
            insert_params['result'] = result
            insert_params['score'] = score
            if target:
                insert_params['target'] = target
            if denominator_result_after_actions:
                insert_params['denominator_result_after_actions'] = denominator_result_after_actions
            if context_id:
                insert_params['context_id'] = context_id
            if identifier_parent:
                insert_params['identifier_parent'] = identifier_parent
                insert_params['should_enter'] = True
            if identifier_result:
                insert_params['identifier_result'] = identifier_result
            if should_enter:
                insert_params['should_enter'] = should_enter
            return insert_params
        except IndexError:
            Log.error('error in build_dictionary_for_db_insert')
            return None

    def get_ps_store_info(self, store_info):
        query = """
                    SELECT s.pk as store_fk, s.additional_attribute_3, r.name as retailer, s.store_number_1,
                    s.business_unit_fk, s.additional_attribute_5, s.district_fk, s.test_store
                    FROM static.stores s
                    left join static.retailer r
                    on r.pk = s.retailer_fk where s.pk = '{}'
                """.format(self.store_id)
        store_data = pd.read_sql_query(query, self.rds_conn.db)
        store_info = store_info.merge(store_data, how='left', left_on='store_fk', right_on='store_fk', suffixes=['', '_1'])
        return store_info


class SimpleFacingsRetriever(BaseFieldRetriever):
    @property
    def sos_field(self):
        return ScifConsts.FACINGS


class DiageoConsts(object):

    SECONDARY_DISPLAYS = ['Secondary', 'Secondary Shelf']
    WRONG_EAN_CODE_LOG = "There isn't matching product fk for ean_code = {}"
    MISSING_ENTITY_IN_TEMPLATE_LOG = "Please fix the template or change the function to support entity = {}"
    MINIMUM_SHELF_TARGETS = {1: ['OTHER'], 2: ['B', 'E'], 3: ['B', 'R', 'E'], 4: ['B', 'R', 'E', 'T'],
                             5: ['B', 'K', 'R', 'E', 'T'], 6: ['B', 'K', 'R', 'E', 'T', 'TT']}
    EMPTY_DATA_LOG = "Unable to calculate shelf placement: Shelf Placement Data or Match Product In Scene are empty"
    ENTITY_TYPE = 'entity type'
    ENTITY_VALUE = 'entity value'
    VSP_LOCATION = 'location'
    WRONG_VALUES_IN_THE_TEMPLATE = "Error was found during row validation! Please check the following: {}, {}, {}"
    SHELVES_GROUPS_CONVERSION = {1: ['E'], 2: ['E', 'T'], 3: ['E', 'R'], 4: ['E', 'T', 'R']}
    SHELF_NUM_FROM_BOTTOM = 'shelf_number_from_bottom'
    SHELF_PLACEMENT_BY_SKU_KPI = 'Vertical Shelf Placement - SKU'
    UNSUPPORTED_ENTITY_LOG = "Shelf placement doesn't support the following entity: {}"
    MISSING_KPI_RESULT_VALUE_LOG = "There isn't any row in static.kpi_result_value table that matching {}"
    SECONDARY_DISPLAY = 'Secondary display'
    SECONDARY_SHELF = SECONDARY = ['Secondary', 'Secondary Shelf']
    CHANNEL = 'Channel'
    TESTED_TYPE = 'Tested Type'
    ANCHOR_TYPE = 'Anchor Type'
    BRAND = 'Brand'
    SUB_BRAND_NAME_RP = 'sub_brand_name'
    TESTED = 'Tested SKU2'
    ANCHOR = 'Anchor SKU2'
    TESTED_PRODUCT_NAME = 'Tested Product Name'
    ANCHOR_PRODUCT_NAME = 'Anchor Product Name'
    TOP_DISTANCE = 'Up to (above) distance (by shelves)'
    BOTTOM_DISTANCE = 'Up to (below) distance (by shelves)'
    LEFT_DISTANCE = 'Up to (Left) Distance (by SKU facings)'
    RIGHT_DISTANCE = 'Up to (right) distance (by SKU facings)'
    LOCATION = 'Primary "In store location"'
    UNLIMITED_DISTANCE = 'General'
    RELATIVE_POSITION_BRAND_TO_BRAND = 'Relative Position - BTB'
    RELATIVE_POSITION_PRODUCT_TO_BRAND = 'Relative Position - PTB'
    RELATIVE_POSITION_SUB_BRAND_TO_SUB_BRAND = 'Relative Position - SBTSB'
    RELATIVE_POSITION_SUB_BRAND_TO_BRAND = 'Relative Position - SBTB'
    RELATIVE_POSITION_PRODUCT_TO_PRODUCT = 'Relative Position - PTP'
    RELATIVE_POSITION = 'Relative Position'
    SUB_BRAND_NAME = 'sub_brand_name'
    MISSING_SUB_BRAND_ENTITY = "sub_brand entity doesn't exist in static.kpi_entity_type!" \
                               "Using static.sub_brand meanwhile"
    SUB_BRAND = 'sub_brand'
    SUB_BRAND_QUERY = """select pk, brand_fk, name from static.sub_brand"""
    WRONG_TEMPLATE_NAME_IN_TEMPLATE_LOG = "The following template exists in the template but not in the DB: {}"
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True

    STRICT_MODE = ALL = 1000
    KPI_NAME = 'Atomic'

    EMPTY = 'Empty'
    DEFAULT = 'Default'
    TOP = 'Top'
    BOTTOM = 'Bottom'
    DIAGEO = 'Diageo'
    BRAND_VARIANT = 'Brand Variant'
    BRAND_NAME_BT = 'Brand Name'
