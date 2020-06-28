from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd
from Projects.RISPARKWINEDE_SAND.Utils.SOS import\
    CategorySecondaryFacingsSOSInStore, ManufacturerSecondaryFacingsSOSPerCategory, \
    BrandOutManufacturerOutCategorySecondaryFacingsSOS, OwnManufacturerPrimaryFacingsSOSInStore, \
    CategoryPrimaryFacingsSOSInStore, ManufacturerPrimaryFacingsSOSPerCategory, \
    BrandOutManufacturerOutCategoryPrimaryFacingsSOS, OwnManufacturerSecondaryFacingsSOSInStore
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from Projects.RISPARKWINEDE_SAND.Data.LocalConsts import Consts


from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment

__author__ = 'limorc'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.assortment = Assortment(self.data_provider)
        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.result_values = self.ps_data.get_result_values()

    def main_calculation(self):

        # facings
        facings_with_stacking_res = self.category_facings(Consts.FACINGS, Consts.FACINGS_STACKING_KPI)
        self.common.save_json_to_new_tables(facings_with_stacking_res)

        facings_ignore_stacking_res = self.category_facings(Consts.FACINGS_IGN_STACK, 'SKU_Facings_ Exclude_Stacking')
        self.common.save_json_to_new_tables(facings_ignore_stacking_res)

        # sos calculation
        sos_res_secondary = self.sos_calculation_secondary()
        self.common.save_json_to_new_tables(sos_res_secondary)
        sos_res_primary = self.sos_calculation_primary()
        self.common.save_json_to_new_tables(sos_res_primary)

        # Assortment base availabilities kpis
        lvl3_result = self.assortment.calculate_lvl3_assortment(False)

        availability_res = self.assortment_calculation(lvl3_result)
        self.common.save_json_to_new_tables(availability_res)

        wine_res = self.wine_availability_calculation(lvl3_result)
        self.common.save_json_to_new_tables(wine_res)

        # commit all results to db
        self.common.commit_results_data()

    def sos_calculation_secondary(self):
        sos_results = []

        manu_from_store_fk = self.common.get_kpi_fk_by_kpi_type('SOS OWN MANUFACTURER OUT OF STORE - SECONDARY SHELF')
        manu_from_store_res = OwnManufacturerSecondaryFacingsSOSInStore(data_provider=self.data_provider,
                                                                        kpi_definition_fk=manu_from_store_fk,
                                                                        categories_include=Consts.SOS_CATEGORY).calculate()
        manu_store_df = self.prepare_table_for_res(manu_from_store_res, False)
        manu_store_df = self.assign_parent_child_identifiers(manu_store_df,
                                                             result_identifier={'val': manu_from_store_fk})
        sos_results.extend(manu_store_df.to_dict('records'))

        category_from_store_fk = self.common.get_kpi_fk_by_kpi_type('SOS CATEGORY OUT OF STORE - SECONDARY SHELF')
        category_from_store_res = CategorySecondaryFacingsSOSInStore(data_provider=self.data_provider,
                                                                     kpi_definition_fk=category_from_store_fk,
                                                                     categories_include=Consts.SOS_CATEGORY).calculate()

        category_from_store_df = self.prepare_table_for_res(category_from_store_res, True)

        category_from_store_df = self.assign_parent_child_identifiers(category_from_store_df, parent_identifier={
            'val': manu_from_store_fk}, result_identifier={'col': ['numerator_id', 'denominator_id'], 'val':
            category_from_store_fk})
        sos_results.extend(category_from_store_df.to_dict('records'))

        manu_from_cat_fk = self.common.get_kpi_fk_by_kpi_type('SOS MANUFACTURER OUT OF CATEGORY - SECONDARY SHELF')
        manu_from_cat_res = ManufacturerSecondaryFacingsSOSPerCategory(data_provider=self.data_provider,
                                                                       kpi_definition_fk=manu_from_cat_fk,
                                                                       categories_include=Consts.SOS_CATEGORY).calculate()
        manu_from_cat_df = self.prepare_table_for_res(manu_from_cat_res, True)

        manu_from_cat_df = self.assign_parent_child_identifiers(manu_from_cat_df,
                                                                parent_identifier={'col': ['denominator_id',
                                                                                           'context_id'], 'val':
                                                                                       category_from_store_fk},
                                                                result_identifier={'col': ['numerator_id',
                                                                                           'denominator_id'], 'val':
                                                                                       manu_from_cat_fk})

        sos_results.extend(manu_from_cat_df.to_dict('records'))

        barnd_out_manu_fk = self.common.get_kpi_fk_by_kpi_type(
            'SOS BRAND OUT OF MANUFACTURER OUT OF CATEGORY - SECONDARY SHELF')
        brand_out_manu_res = BrandOutManufacturerOutCategorySecondaryFacingsSOS(data_provider=self.data_provider,
                                                                                kpi_definition_fk=barnd_out_manu_fk,
                                                                                categories_include=Consts.SOS_CATEGORY).calculate()
        brand_out_manu_df = self.prepare_table_for_res(brand_out_manu_res, True)
        brand_out_manu_df = self.assign_parent_child_identifiers(brand_out_manu_df,
                                                                 parent_identifier={'col':
                                                                                        ['denominator_id',
                                                                                         'context_id'],
                                                                                    'val': manu_from_cat_fk})
        sos_results.extend(brand_out_manu_df.to_dict('records'))

        return sos_results

    def sos_calculation_primary(self):
        sos_results = []

        manu_from_store_fk = self.common.get_kpi_fk_by_kpi_type(
            'SOS OWN MANUFACTURER OUT OF STORE - PRIMARY SHELF')
        manu_from_store_res = OwnManufacturerPrimaryFacingsSOSInStore(data_provider=self.data_provider,
                                                                      kpi_definition_fk=manu_from_store_fk,
                                                                      categories_include=Consts.SOS_CATEGORY).calculate()
        manu_store_df = self.prepare_table_for_res(manu_from_store_res, False)
        manu_store_df = self.assign_parent_child_identifiers(manu_store_df,
                                                             result_identifier={'val': manu_from_store_fk})
        sos_results.extend(manu_store_df.to_dict('records'))

        category_from_store_fk = self.common.get_kpi_fk_by_kpi_type('SOS CATEGORY OUT OF STORE - PRIMARY SHELF')
        category_from_store_res = CategoryPrimaryFacingsSOSInStore(data_provider=self.data_provider,
                                                                   kpi_definition_fk=category_from_store_fk,
                                                                   categories_include=Consts.SOS_CATEGORY).calculate()
        category_from_store_df = self.prepare_table_for_res(category_from_store_res, True)
        category_from_store_df = self.assign_parent_child_identifiers(category_from_store_df, parent_identifier={
            'val': manu_from_store_fk}, result_identifier={'col': ['numerator_id', 'denominator_id'], 'val':
            category_from_store_fk})
        sos_results.extend(category_from_store_df.to_dict('records'))

        manu_from_cat_fk = self.common.get_kpi_fk_by_kpi_type('SOS MANUFACTURER OUT OF CATEGORY - PRIMARY SHELF')
        manu_from_cat_res = ManufacturerPrimaryFacingsSOSPerCategory(data_provider=self.data_provider,
                                                                     kpi_definition_fk=manu_from_cat_fk,
                                                                     categories_include=Consts.SOS_CATEGORY).calculate()
        manu_from_cat_df = self.prepare_table_for_res(manu_from_cat_res, True)
        manu_from_cat_df = self.assign_parent_child_identifiers(manu_from_cat_df,
                                                                parent_identifier={'col': ['denominator_id',
                                                                                           'context_id'], 'val':
                                                                                       category_from_store_fk},
                                                                result_identifier={'col': ['numerator_id',
                                                                                           'denominator_id'], 'val':
                                                                                       manu_from_cat_fk})
        sos_results.extend(manu_from_cat_df.to_dict('records'))

        barnd_out_manu_fk = self.common.get_kpi_fk_by_kpi_type(
            'SOS BRAND OUT OF MANUFACTURER OUT OF CATEGORY - PRIMARY SHELF')
        brand_out_manu_res = BrandOutManufacturerOutCategoryPrimaryFacingsSOS(data_provider=self.data_provider,
                                                                              kpi_definition_fk=barnd_out_manu_fk,
                                                                              categories_include=Consts.SOS_CATEGORY).calculate()
        brand_out_manu_df = self.prepare_table_for_res(brand_out_manu_res, True)
        brand_out_manu_df = self.assign_parent_child_identifiers(brand_out_manu_df,
                                                                 parent_identifier={'col':
                                                                                        ['denominator_id',
                                                                                         'context_id'],
                                                                                    'val': manu_from_cat_fk})
        sos_results.extend(brand_out_manu_df.to_dict('records'))
        return sos_results

    def assign_parent_child_identifiers(self, df, parent_identifier=None, result_identifier=None):
        """
        This function  extract from parent identifier and result identifier the values from the dict by keys:val and col
        It sendt it to assign_cols func which add the identifiers to the df
        """
        if parent_identifier:
            df = self.assign_cols(df, 'identifier_parent', parent_identifier.get('col', None),
                                  parent_identifier.get('val',
                                                        ''))
        if result_identifier:
            df = self.assign_cols(df, 'identifier_result', result_identifier.get('col', None),
                                  result_identifier.get('val', ''))
        return df

    @staticmethod
    def assign_cols(df, identifier, col_to_assign, val_to_assign):
        """
        function add identifer column and determine the column value  ,
        identifier will be string = combination of other columns in df and value received
        :param df:
        :param identifier: string 'identifier_parent'/ 'identifier_result'
        :param col_to_assign: array of columns name.
        :param val_to_assign: string/int
        :return:
        """
        if col_to_assign:
            df.loc[:, identifier] = df[col_to_assign].apply(lambda x: ' '.join(x.astype(str)), axis=1) + ' ' + str(
                val_to_assign)
        else:
            df.loc[:, identifier] = str(val_to_assign)

        return df

    @staticmethod
    def prepare_table_for_res(res_dict, should_enter):
        """This function takes the df results which returns from sos classes and change it to be a db result dict"""
        df = pd.DataFrame(
            [res.to_dict if type(res) != dict else res for res in res_dict])  # need to add parent identifer
        df.loc[:, 'session_fk'] = df['fk']
        df.loc[:, 'fk'] = df['kpi_definition_fk']
        df.loc[:, 'should_enter'] = should_enter

        return df

    def wine_availability_calculation(self, lvl3_result):
        """This function calcualte the wine availability in sku level and store level"""
        wine_res = []
        lvl3_result = self.filter_by_kpis(lvl3_result, Consts.WINE_LEVEL_2_NAMES)
        if lvl3_result.empty:
            return wine_res
        wine_sku = self.distribution_sku_level(lvl3_result, Consts.WINE_SKU_LVL, {'col': ['kpi_fk_lvl2']})
        wine_res.extend(wine_sku.to_dict('records'))
        store_res = self.wine_store_assortment(lvl3_result, Consts.WINE_STORE_LEVEL)
        wine_res.extend(store_res.to_dict('records'))
        return wine_res

    def filter_by_kpis(self, lvl3_result, kpi_types):
        """ filter assortment df from irrelevant kpis """
        distribution_kpis = [self.common.get_kpi_fk_by_kpi_type(kpi_name) for kpi_name in kpi_types]
        lvl3_result = lvl3_result[lvl3_result['kpi_fk_lvl2'].isin(distribution_kpis)]
        return lvl3_result

    def assortment_category(self, lvl3_result):
        """ Combine assortment df to lvl3_result"""
        cat_df = self.all_products[['product_fk', 'category_fk']]
        lvl3_with_cat = lvl3_result.merge(cat_df, on='product_fk', how='left')
        lvl3_with_cat = lvl3_with_cat[lvl3_with_cat['category_fk'].notnull()]
        return lvl3_with_cat

    def assortment_calculation(self, lvl3_result):
        ava_res = []
        lvl3_result = self.filter_by_kpis(lvl3_result, Consts.DISTRIBUTION_LEVEL_2_NAMES)

        if lvl3_result.empty:
            return ava_res
        # level 3 results  -  SKU LEVEL
        lvl3_with_cat = self.assortment_category(lvl3_result)

        sku_results = self.distribution_sku_level(lvl3_with_cat, Consts.DIST_SKU_LVL,
                                                  {'col': ['kpi_fk_lvl2', 'category_fk'], 'val': Consts.DISTRIBUTION})

        ava_res = self.append_to_res(ava_res, sku_results, Consts.SKU_LEVEL)

        oos_sku_res = self.sku_oos(sku_results)
        ava_res = self.append_to_res(ava_res, oos_sku_res, Consts.SKU_LEVEL)

        #  calculate level 2 results -  GROUP LEVEL
        group_result = self.calculate_lvl2_assortment(lvl3_with_cat, Consts.LVL2_GROUP_HEADERS)
        oos_group_res = self.calculate_oos_lvl2(group_result)

        group_result = self.assign_parent_child_identifiers(group_result, parent_identifier={'col': ['category_fk'],
                                                                                             'val': Consts.DISTRIBUTION},
                                             result_identifier={'col': ['kpi_fk_lvl2',
                                                                        'category_fk'], 'val': Consts.DISTRIBUTION})
        group_result = self.group_level(group_result, Consts.DIST_GROUP_LVL)
        oos_group_res = self.assign_parent_child_identifiers(oos_group_res, parent_identifier={'col': ['category_fk'],
                                                                                               'val': Consts.OOS},
                                             result_identifier={'col': ['kpi_fk_lvl2', 'category_fk'], 'val':
                                                 Consts.OOS})
        oos_group_res = self.group_level(oos_group_res, Consts.OOS_GROUP_LVL)
        ava_res.extend(group_result.to_dict('records'))
        ava_res.extend(oos_group_res.to_dict('records'))

        # calculate - CATEGORY LEVEL
        category_result = self.calculate_category_result(group_result, Consts.DISTRIBUTION)
        ava_res.extend(category_result.to_dict('records'))

        category_result_oos = self.calculate_category_result(oos_group_res, Consts.OOS)
        ava_res.extend(category_result_oos.to_dict('records'))

        # calculate - STORE LEVEL
        store_result = self.calculate_store_assortment(category_result, Consts.DIST_STORE_LVL)
        oos_store_result = self.calculate_store_assortment(category_result_oos, Consts.OOS_STORE_LVL)
        ava_res.extend(store_result.to_dict('records'))
        ava_res.extend(oos_store_result.to_dict('records'))

        return ava_res

    def distribution_sku_level(self, lvl_3_result, kpi_name, identifiers):
        """ This function receive df = lvl_3_result assortment with data regarding the assortment products
            This function turn the sku_assortment_results to be in a shape of db result.
            return distribution_db_results df
        """
        sku_results = lvl_3_result.rename(
            columns={'product_fk': 'numerator_id', 'assortment_group_fk': 'denominator_id',
                     'in_store': 'result', 'kpi_fk_lvl3': 'fk', 'facings':
                         'numerator_result'}, inplace=False)
        sku_results.loc[:, 'result'] = sku_results.apply(lambda row: self.kpi_result_value(row.result), axis=1)
        sku_results = sku_results.assign(denominator_result=1,
                                         score=sku_results['result'])
        sku_results.loc[:, 'should_enter'] = True
        sku_results['fk'] = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        self.assign_parent_child_identifiers(sku_results, identifiers)
        Log.info('Distribution sku level is done ')
        return sku_results

    def sku_oos(self, sku_results):
        oos_res = sku_results[sku_results['result'] == self.kpi_result_value(0)]
        oos_res.loc[:, 'fk'] = self.common.get_kpi_fk_by_kpi_type(Consts.OOS_SKU_LVL)
        self.assign_parent_child_identifiers(oos_res, {'col': ['kpi_fk_lvl2', 'category_fk'], 'val': 'OOS'})
        return oos_res

    def group_level(self, lvl_2_result, kpi_name):
        """ This function receive df = lvl_3_result assortment with data regarding the assortment products
            This function turn the sku_assortment_results to be in a shape of db result.
            return distribution_db_results df
        """
        group_results = lvl_2_result.rename(
            columns={'kpi_fk_lvl2': 'numerator_id', 'category_fk': 'denominator_id',
                     'total': 'denominator_result', 'passes': 'numerator_result'}, inplace=False)
        group_results.loc[:, 'result'] = group_results['numerator_result'] / group_results['denominator_result']
        group_results = group_results.assign(score=group_results['result'])
        group_results.loc[:, 'fk'] = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        group_results = self.filter_df_by_col(group_results, Consts.GROUPS_LEVEL)
        Log.info('Group level is done ')
        return group_results

    def filter_df_by_col(self, df, level):
        """
        :param df: df results lvl2 / lvl3 assortment results
        :param level: sku /  group level
        :return:filtered df
        """
        if level == Consts.SKU_LEVEL:
            return df[Consts.LVL3_SESSION_RESULTS_COL]

        if level == Consts.GROUPS_LEVEL:
            return df[Consts.LVL2_SESSION_RESULTS_COL]

        if level == Consts.STORE_LEVEL:
            return df[Consts.LVL1_SESSION_RESULTS_COL]

    def kpi_result_value(self, value):
        """
        :param value:  availability kpi result 0 for oos and 1 for distrbution
         Function retrieve the kpi_result_value needed for Availability KPIS
        (kpi result value match to mobile report signs) , according to the kpi result.
        :return pk of kpi_result_value
         """
        value = Consts.OOS if value == 0 else Consts.DISTRIBUTION
        value_info = self.result_values[self.result_values['value'] == value]
        if value_info.empty:
            return
        return value_info.pk.iloc[0]

    @staticmethod
    def calculate_oos_lvl2(lvl2_res):
        """ create df for oos level based on distribution results
            :param lvl2_res - assortment df results in sku level
        """
        oos_res = lvl2_res.copy()
        oos_res['passes'] = oos_res['total'] - oos_res['passes']
        return oos_res

    @staticmethod
    def calculate_lvl2_assortment(lvl3_assortment, group_by_cols):
        """
        :param lvl3_assortment: return value of 'calculate_lvl3_assortment' func.
        :return: data frame on the assortment group level with the following fields:
        ['assortment_super_group_fk', 'assortment_group_fk', 'assortment_fk', 'target', 'passes',
        'total', 'kpi_fk_lvl1', 'kpi_fk_lvl2', 'group_target_date', 'super_group_target'].
        Indicates for each assortment group how many products were in the store (passes) out of the total\ target
        (total\ target).
        """
        lvl3_with_cat = lvl3_assortment.copy()
        lvl3_with_cat = lvl3_with_cat.fillna(Consts.EMPTY_VAL)
        lvl2_res = lvl3_with_cat.groupby(group_by_cols)['in_store'].agg(
            [('total', 'count'), ('passes', 'sum')]).reset_index()
        lvl2_res.loc[:, 'should_enter'] = True
        return lvl2_res

    def calculate_category_result(self, group_result, parent_iden):
        """Create df results for assortment kpi in category level
            :param group_result - db results for assortment kpi in group level
            :param parent_iden  - 'OOS'/'DIST  , kpi identifier between oos and dist
        """
        category_result = group_result.groupby(['denominator_id'])['numerator_result', 'denominator_result'].agg(
            {'numerator_result': 'sum', 'denominator_result': 'sum'}).reset_index()
        category_result.loc[:, 'result'] = category_result['numerator_result'] / category_result['denominator_result']
        category_result = category_result.rename(columns={'denominator_id': 'numerator_id'},
                                                 inplace=False)
        category_result.loc[:, 'denominator_id'] = self.manufacturer_fk
        kpi_name = Consts.OOS_CAT_LVL if parent_iden == Consts.OOS else Consts.DIST_CAT_LVL
        category_result.loc[:, 'fk'] = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        category_result = category_result.assign(score=category_result['result'])
        category_result = self.assign_parent_child_identifiers(category_result,
                                                               result_identifier={'col': ['numerator_id'], 'val': parent_iden},
                                                               parent_identifier={'val': parent_iden})
        category_result.loc[:, 'should_enter'] = True
        return category_result

    def calculate_store_assortment(self, lvl_2_res, kpi_name):
        """ Calculate  assortment  in store level
            :param lvl_2_res- lvl2 result , df of results from granular group level/ category level
            :param kpi_name - kpi type
        """
        store_assortment = lvl_2_res.groupby(['fk'])['numerator_result', 'denominator_result'].agg(
            {'numerator_result': 'sum', 'denominator_result': 'sum'}).reset_index()

        store_assortment.loc[:, 'result'] = store_assortment['numerator_result'] / store_assortment['denominator_result']
        store_assortment.loc[:, 'denominator_id'] = self.store_id
        store_assortment.loc[:, 'numerator_id'] = self.manufacturer_fk
        store_assortment.loc[:, 'fk'] = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        store_assortment.loc[:, 'should_enter'] = False
        kpi_identifier = Consts.DISTRIBUTION if kpi_name == Consts.DIST_STORE_LVL else Consts.OOS
        self.assign_parent_child_identifiers(store_assortment, result_identifier={'val': kpi_identifier})

        return store_assortment

    def wine_store_assortment(self, wine_lvl3_res, kpi_type):
        """ Create df results for wine assortment kpi in store level
            :param kpi_type - kpi name
            :param  wine_lvl3_res - df of lvl3_results
        """
        lvl2_result = self.calculate_lvl2_assortment(wine_lvl3_res, Consts.WINE_GROUP_HEADERS)
        self.assign_parent_child_identifiers(lvl2_result, result_identifier={'col': ['kpi_fk_lvl2']})
        lvl2_result.loc[:, 'denominator_id'] = self.store_id
        lvl2_result.loc[:, 'numerator_id'] = self.manufacturer_fk
        lvl2_result = lvl2_result.rename(
            columns={'total': 'denominator_result', 'passes': 'numerator_result'}, inplace=False)
        lvl2_result.loc[:, 'fk'] = self.common.get_kpi_fk_by_kpi_type(kpi_type)
        lvl2_result.loc[:, 'result'] = lvl2_result['numerator_result'] / lvl2_result['denominator_result']
        store_results = lvl2_result.assign(score=lvl2_result['result'])
        store_results = self.filter_df_by_col(store_results, Consts.STORE_LEVEL)
        return store_results

    def category_facings(self, facings, kpi_name):
        """ return df results with facings count per kpi
            :param facings : 'facings' / 'facings'
            :param kpi_name : kpi type from df
        """
        filterd_scif = self.scif[~ self.scif['product_type'].isin(Consts.IGNORE_PRODUCT_TYPE) & self.scif[
            'category'].isin(Consts.FACINGS_CATEGORIES)]
        results = filterd_scif.groupby(['product_fk'])[facings].agg(
            {facings: 'sum'}).reset_index()
        results.loc[:, 'denominator_id'] = self.store_id
        results.loc[:, 'fk'] = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        results = results.rename(columns={facings: 'result', 'numerator_id': 'product_fk'})
        results = results.assign(score=results['result'])
        return results.to_dict('records')

    def append_to_res(self, total_res, res, level):
        sku_results = self.filter_df_by_col(res, level)
        total_res.extend(sku_results.to_dict('records'))
        return total_res
