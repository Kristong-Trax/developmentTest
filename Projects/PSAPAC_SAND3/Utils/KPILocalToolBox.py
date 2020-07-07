from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import GSKToolBox
from Trax.Utils.Logging.Logger import Log
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ProductsConsts
from KPIUtils.GlobalProjects.GSK.Data.LocalConsts import Consts
import pandas as pd
import numpy as np
import math

__author__ = 'prasanna'


class GSKLocalToolBox(GSKToolBox, object):

    def __init__(self, data_provider, output, common, set_up_file):
        super(GSKLocalToolBox, self).__init__(data_provider, output, common, set_up_file)

    def assortment_lvl3_gsk_adjustments1(self):
        super(GSKLocalToolBox, self).assortment_lvl3_gsk_adjustments1()

        if self.assort_lvl3 is None or self.assort_lvl3.empty:
            return

        # TODO: Multiple-granular groups for a store's policy - Remove Duplicate product_fks if Any
        Log.info("Dropping duplicate product_fks across multiple-granular groups")
        Log.info("Before : {}".format(len(self.assort_lvl3)))
        self.assort_lvl3 = self.assort_lvl3.drop_duplicates(subset=[ProductsConsts.PRODUCT_FK])
        Log.info("Product in asssort: {}".format( (~(self.assort_lvl3.kpi_fk_lvl3.isnull())).sum() ) )
        Log.info("Product extra: {}".format(self.assort_lvl3.kpi_fk_lvl3.isnull().sum()))
        Log.info("After : {}".format(len(self.assort_lvl3)))

    def availability_calculation(self, availability_type):
        """
            :param availability_type : "Store" , "Category", "SubCategory"
            Function initialize assortment_level_3 (class attribute) if not initialized before
            and calculate availability results according to availability type
        """

        self.extract_data_set_up_file("availability")
        if self.assort_lvl3 is None:
            self.assortment_lvl3_gsk_adjustments1()
            if self.assort_lvl3 is None or self.assort_lvl3.empty:
                return
        dict_list = []
        if availability_type == Consts.STORE:
            return self.assortment_calculation(self.assort_lvl3, self.store_fk, Consts.STORE)

        if availability_type == Consts.SUB_CATEGORY:
            categories = self.assort_lvl3[ProductsConsts.CATEGORY_FK].dropna().unique()
            for cat in categories:
                assort_lvl3 = self.assort_lvl3[self.assort_lvl3[ProductsConsts.CATEGORY_FK] == cat]
                if assort_lvl3.empty:
                    continue
                if 'total' not in self.assortment.LVL2_HEADERS or 'passes' not in self.assortment.LVL2_HEADERS:
                    self.assortment.LVL2_HEADERS.extend(['total', 'passes'])
                dict_list.extend(self.assortment_calculation(assort_lvl3, cat, Consts.SUB_CATEGORY_PARENT))
                sub_categories = assort_lvl3[ProductsConsts.SUB_CATEGORY_FK].dropna().unique()
                for sub_cat in sub_categories:
                    assort_lvl3_sub_cat = assort_lvl3[assort_lvl3[ProductsConsts.SUB_CATEGORY_FK] == sub_cat]
                    if 'total' not in self.assortment.LVL2_HEADERS or 'passes' not in self.assortment.LVL2_HEADERS:
                         self.assortment.LVL2_HEADERS.extend(['total', 'passes'])
                    dict_list.extend(self.assortment_calculation(assort_lvl3_sub_cat, sub_cat, Consts.SUB_CATEGORY))

        if availability_type == Consts.CATEGORY:
            categories = self.all_products[ProductsConsts.CATEGORY_FK].dropna().unique()
            for cat in categories:
                assort_lvl3 = self.assort_lvl3[self.assort_lvl3[ProductsConsts.CATEGORY_FK] == cat]
                if 'total' not in self.assortment.LVL2_HEADERS or 'passes' not in self.assortment.LVL2_HEADERS:
                    self.assortment.LVL2_HEADERS.extend(['total', 'passes'])
                dict_list.extend(self.assortment_calculation(assort_lvl3, cat, Consts.CATEGORY))

        return dict_list

    def assortment_calculation(self, assort_lvl3, denominator_fk, availability_type):
        """
           :param denominator_fk  : store_fk/category_fk/sub_category_fk
           :param assort_lvl3  : df of  level 3 assortment calculation
           :param availability_type : "Store"/"Category"/"SubCategory"
           :return results_df that contains:
               - result in SKU level :  distribution 1/2/3  oos 1/2
               - result : stock rate for oos and distribution

        """
        dict_list = []

        oos_fk = self.common.get_kpi_fk_by_kpi_type(
            Consts.OOS_PREFIX + Consts.AVAILABILITY_DICT[('OOS', availability_type)][0])
        dist_fk = self.common.get_kpi_fk_by_kpi_type(
            Consts.DISTRIBUTION_PREFIX + Consts.AVAILABILITY_DICT[('DIST', availability_type)][0])
        category_fk = None
        if availability_type is Consts.SUB_CATEGORY:
            category_fk = assort_lvl3[ProductsConsts.CATEGORY_FK].iloc[0]

        if not assort_lvl3.empty:
            if availability_type is not Consts.SUB_CATEGORY_PARENT:
                oos_sku_fk = self.common.get_kpi_fk_by_kpi_type(
                    Consts.OOS_SKU_PREFIX + Consts.AVAILABILITY_DICT[('OOS', availability_type)][1])
                dist_sku_fk = self.common.get_kpi_fk_by_kpi_type(
                    Consts.DISTRIBUTION_SKU_PREFIX + Consts.AVAILABILITY_DICT[('DIST', availability_type)][1])
                dist_origin_kpi = self.common.get_kpi_fk_by_kpi_name(Consts.DISTRIBUTION)

                for result in assort_lvl3.itertuples():
                    if (math.isnan(result.in_store)) | (result.kpi_fk_lvl3 != dist_origin_kpi):
                        score = self.result_value_pk(Consts.EXTRA)
                        result_num = 1
                    else:
                        score = self.result_value_pk(Consts.OOS) if result.in_store == 0 else self.result_value_pk \
                            (Consts.DISTRIBUTED)
                        result_num = result.in_store
                    last_status = self.get_last_status(dist_sku_fk, result.product_fk)
                    dict_list.append(
                        self.build_dictionary_for_db_insert_v2(
                            fk=dist_sku_fk, numerator_id=result.product_fk, numerator_result=result_num,
                            result=score, score=last_status, denominator_id=denominator_fk, denominator_result=1,
                            identifier_parent=self.common.get_dictionary(kpi_parent=dist_fk,
                                                                         type=availability_type,
                                                                         type_fk=denominator_fk)))

                    # OOS
                    if score == self.result_value_pk(Consts.OOS):
                        dict_list.append(self.build_dictionary_for_db_insert_v2(
                            fk=oos_sku_fk, numerator_id=result.product_fk, numerator_result=score, result=score,
                            score=score
                            , denominator_id=denominator_fk, denominator_result=1, identifier_parent=
                            self.common.get_dictionary(kpi_parent=oos_fk, type=availability_type,
                                                       type_fk=denominator_fk)))

            assort_lvl3 = assort_lvl3[~assort_lvl3['in_store'].isnull()]
            if assort_lvl3.empty:
                dict_list.append(self.build_dictionary_for_db_insert_v2(
                    fk=dist_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                    result=0, score=0, denominator_id=denominator_fk,
                    denominator_result=0,
                    identifier_result=self.common.get_dictionary(kpi_parent=dist_fk, type=availability_type,
                                                                 type_fk=denominator_fk)))
                return dict_list
            oos_numerator = len(assort_lvl3[assort_lvl3['in_store'] == 0])
            denominator = len(assort_lvl3)
            oos_res = round(np.divide(float(oos_numerator), float(denominator)), 4)
            if availability_type is Consts.SUB_CATEGORY:
                oos_parent_fk = self.common.get_kpi_fk_by_kpi_type(
                    Consts.OOS_PREFIX + Consts.AVAILABILITY_DICT[('OOS', Consts.SUB_CATEGORY_PARENT)][0])
                dict_list.append(self.build_dictionary_for_db_insert_v2(
                    fk=oos_fk, numerator_id=self.manufacturer_fk, numerator_result=oos_numerator, result=oos_res,
                    score=oos_res, denominator_result=denominator, denominator_id=denominator_fk, identifier_result=
                    self.common.get_dictionary(kpi_parent=oos_fk, type=availability_type, type_fk=denominator_fk),
                    identifier_parent=self.common.get_dictionary(kpi_parent=oos_parent_fk,
                                                                 type=Consts.SUB_CATEGORY_PARENT, type_fk=category_fk)))
            else:
                dict_list.append(self.build_dictionary_for_db_insert_v2(
                    fk=oos_fk, numerator_id=self.manufacturer_fk, numerator_result=oos_numerator, result=oos_res,
                    score=oos_res, denominator_result=denominator, denominator_id=denominator_fk, identifier_result=
                    self.common.get_dictionary(kpi_parent=oos_fk, type=availability_type, type_fk=denominator_fk)))

            # Modified for Multi-Granular Group Assortment
            lvl2_result = self.assortment.calculate_lvl2_assortment(assort_lvl3)
            result_passes = result_total = denominator_res = 0
            for result in lvl2_result.itertuples():
                result_passes += result.passes
                result_total += result.total
                if not pd.isnull(result.target) and not pd.isnull(
                        result.group_target_date) and result.group_target_date <= self.current_date:
                    denominator_res += result.target
                else:
                    denominator_res += result.total

            res = round(np.divide(float(result_passes), float(denominator_res)), 4)
            if res >= 1:
                score = 1
            else:
                score = 0
            res = 1 if res > 1 else res
            if availability_type is Consts.SUB_CATEGORY:
                dist_parent_fk = self.common.get_kpi_fk_by_kpi_type(
                    Consts.DISTRIBUTION_PREFIX + Consts.AVAILABILITY_DICT[('DIST', Consts.SUB_CATEGORY_PARENT)][0])
                dict_list.append(self.build_dictionary_for_db_insert_v2(
                    fk=dist_fk, numerator_id=self.manufacturer_fk, numerator_result=result_passes,
                    result=res, score=score, denominator_id=denominator_fk,
                    denominator_result=result_total, identifier_result=
                    self.common.get_dictionary(kpi_parent=dist_fk, type=availability_type, type_fk=denominator_fk),
                    identifier_parent=
                    self.common.get_dictionary(kpi_parent=dist_parent_fk, type=Consts.SUB_CATEGORY_PARENT,
                                               type_fk=category_fk)))
            else:
                dict_list.append(self.build_dictionary_for_db_insert_v2(
                    fk=dist_fk, numerator_id=self.manufacturer_fk, numerator_result=result_passes,
                    result=res, score=score, denominator_id=denominator_fk,
                    denominator_result=result_total, identifier_result=
                    self.common.get_dictionary(kpi_parent=dist_fk, type=availability_type, type_fk=denominator_fk)))

        return dict_list

