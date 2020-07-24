from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from Projects.STRAUSSIL.Data.LocalConsts import Consts
from Trax.Data.ProfessionalServices.PsConsts.PS import ExternalTargetsConsts
from KPIUtils_v2.Utils.Parsers import ParseInputKPI as Parser
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'ilays'


class ToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.own_manufacturer_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.parser = Parser
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.assortment = Assortment(self.data_provider, self.output)
        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.kpi_external_targets = self.ps_data.get_kpi_external_targets(key_fields=Consts.KEY_FIELDS,
                                                                          data_fields=Consts.DATA_FIELDS)

    def main_calculation(self):
        self.calculate_score_sos()
        self.calculate_oos_and_distribution(assortment_type="Core")
        self.calculate_oos_and_distribution(assortment_type="Launch")
        self.calculate_oos_and_distribution(assortment_type="Focus")
        self.calculate_hierarchy_sos(calculation_type='FACINGS')
        self.calculate_hierarchy_sos(calculation_type='LINEAR')

    @kpi_runtime()
    def calculate_oos_and_distribution(self, assortment_type):
        dis_numerator = total_facings = 0
        oos_store_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=assortment_type + Consts.OOS)
        oos_sku_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=assortment_type + Consts.OOS_SKU)
        dis_store_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=assortment_type + Consts.DISTRIBUTION)
        dis_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=assortment_type + Consts.DISTRIBUTION_CAT)
        dis_sku_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=assortment_type + Consts.DISTRIBUTION_SKU)
        assortment_df = self.assortment.get_lvl3_relevant_ass()
        assortment_df = assortment_df[assortment_df['kpi_fk_lvl3'] == dis_sku_kpi_fk]
        product_fks = assortment_df['product_fk'].tolist()
        categories = list(set(self.all_products[self.all_products['product_fk'].isin(product_fks)]['category_fk']))
        categories_dict = dict.fromkeys(categories, (0, 0))

        # sku level distribution
        for sku in product_fks:
            # 2 for distributed and 1 for oos
            category_fk = self.all_products[self.all_products['product_fk'] == sku]['category_fk'].values[0]
            product_df = self.scif[self.scif['product_fk'] == sku]
            if product_df.empty:
                categories_dict[category_fk] = map(sum, zip(categories_dict[category_fk], [0, 1]))
                result = 0
                facings = 0
                # Saving OOS only if product wasn't in store
                self.common.write_to_db_result(fk=oos_sku_kpi_fk, numerator_id=sku, denominator_id=category_fk,
                                               result=result, numerator_result=result, denominator_result=result,
                                               score=facings, identifier_parent=assortment_type + "_OOS",
                                               should_enter=True)
            else:
                categories_dict[category_fk] = map(sum, zip(categories_dict[category_fk], [1, 1]))
                result = 1
                facings = product_df['facings'].values[0]
                dis_numerator += 1
                total_facings += facings
            self.common.write_to_db_result(fk=dis_sku_kpi_fk, numerator_id=sku, denominator_id=category_fk,
                                           result=result, numerator_result=result, denominator_result=result,
                                           score=facings, should_enter=True,
                                           identifier_parent=assortment_type + "_DIS_CAT_{}".format(str(category_fk)))

        # category level distribution
        for category_fk in categories_dict.keys():
            cat_numerator, cat_denominator = categories_dict[category_fk]
            cat_result = self.get_result(cat_numerator, cat_denominator)
            self.common.write_to_db_result(fk=dis_cat_kpi_fk, numerator_id=category_fk,
                                           denominator_id=self.store_id, result=cat_result, should_enter=True,
                                           numerator_result=cat_numerator, denominator_result=cat_denominator,
                                           score=cat_result, identifier_parent=assortment_type + "_DIS",
                                           identifier_result=assortment_type + "_DIS_CAT_{}".format(str(category_fk)))

        # store level oos and distribution
        denominator = len(product_fks)
        dis_result = self.get_result(dis_numerator, denominator)
        if denominator == 0:
            oos_result = dis_result = None
        else:
            oos_result = 1 - dis_result
        oos_numerator = denominator - dis_numerator
        self.common.write_to_db_result(fk=oos_store_kpi_fk, numerator_id=self.own_manufacturer_fk,
                                       denominator_id=self.store_id, result=oos_result, numerator_result=oos_numerator,
                                       denominator_result=denominator, score=total_facings,
                                       identifier_result=assortment_type + "_OOS")
        self.common.write_to_db_result(fk=dis_store_kpi_fk, numerator_id=self.own_manufacturer_fk,
                                       denominator_id=self.store_id, result=dis_result, numerator_result=dis_numerator,
                                       denominator_result=denominator, score=total_facings,
                                       identifier_result=assortment_type + "_DIS")

    def get_kpi_fks(self, kpis_list):
        for kpi in kpis_list:
            self.common.get_kpi_fk_by_kpi_type(kpi_type=kpi)

    @staticmethod
    def calculate_sos_res(numerator, denominator):
        if denominator == 0:
            return 0, 0, 0
        result = round(numerator / float(denominator), 3)
        return result, numerator, denominator

    @kpi_runtime()
    def calculate_score_sos(self):
        relevant_template = self.kpi_external_targets[self.kpi_external_targets[ExternalTargetsConsts.OPERATION_TYPE]
                                                      == Consts.SOS_KPIS]
        relevant_rows = relevant_template.copy()
        lsos_score_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.LSOS_SCORE_KPI)
        store_denominator = len(relevant_rows)
        store_numerator = 0
        for i, kpi_row in relevant_template.iterrows():
            kpi_fk, num_type, num_value, deno_type, deno_value, target, target_range = kpi_row[Consts.RELEVANT_FIELDS]
            numerator_filters, denominator_filters = self.get_num_and_den_filters(num_type, num_value, deno_type,
                                                                                  deno_value)
            # Only straussil SKUs
            numerator_filters['manufacturer_fk'] = self.own_manufacturer_fk
            denominator_filters['manufacturer_fk'] = self.own_manufacturer_fk
            numerator_df = self.parser.filter_df(conditions=numerator_filters, data_frame_to_filter=self.scif)
            denominator_df = self.parser.filter_df(conditions=denominator_filters, data_frame_to_filter=self.scif)
            numerator_result = numerator_df['gross_len_ign_stack'].sum()
            denominator_result = denominator_df['gross_len_ign_stack'].sum()
            lsos_result = self.get_result(numerator_result, denominator_result)
            score = 1 if ((target - target_range) <= lsos_result <= (target + target_range)) else 0
            store_numerator += score
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.own_manufacturer_fk,
                                           denominator_id=self.store_id, should_enter=True, target=target,
                                           numerator_result=numerator_result, denominator_result=denominator_result,
                                           result=lsos_result, score=score, identifier_parent='LSOS_SCORE',
                                           weight=target_range)
        store_result = self.get_result(store_numerator, store_denominator)
        self.common.write_to_db_result(fk=lsos_score_kpi_fk, numerator_id=self.own_manufacturer_fk,
                                       denominator_id=self.store_id, should_enter=True, target=store_denominator,
                                       numerator_result=store_numerator, denominator_result=store_denominator,
                                       result=store_numerator, score=store_result, identifier_result='LSOS_SCORE')

    @staticmethod
    def get_num_and_den_filters(numerator_type, numerator_value, denominator_type, denominator_value):
        if type(numerator_value) != list:
            numerator_value = [numerator_value]
        if type(denominator_value) != list:
            denominator_value = [denominator_value]
        numerator_filters = {numerator_type: numerator_value}
        denominator_filters = {denominator_type: denominator_value}
        return numerator_filters, denominator_filters

    @staticmethod
    def get_result(numerator, denominator):
        if denominator == 0:
            return 0
        else:
            return round(numerator / float(denominator), 2)

    def calculate_hierarchy_sos(self, calculation_type):
        brand_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=(calculation_type + Consts.SOS_BY_BRAND))
        brand_category_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=(calculation_type +
                                                                             Consts.SOS_BY_CAT_BRAND))
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=(calculation_type + Consts.SOS_BY_CAT_BRAND_SKU))
        calculation_param = "facings_ign_stack" if calculation_type == 'FACINGS' else "gross_len_ign_stack"
        sos_df = self.parser.filter_df(conditions={'rlv_sos_sc': 1, 'product_type': ['SKU', 'Empty', 'Other']},
                                       data_frame_to_filter=self.scif)
        # brand level sos
        session_brands = set(sos_df['brand_fk'])
        brand_den = sos_df[calculation_param].sum()
        for brand_fk in session_brands:
            filters = {'brand_fk': brand_fk}
            brand_df = self.parser.filter_df(conditions=filters, data_frame_to_filter=sos_df)
            if brand_df.empty:
                continue
            manufacturer_fk = brand_df['manufacturer_fk'].values[0]
            brand_num = brand_df[calculation_param].sum()
            if brand_num == 0:
                continue
            brand_res, brand_num, brand_den = self.calculate_sos_res(brand_num, brand_den)
            self.common.write_to_db_result(fk=brand_kpi_fk, numerator_id=brand_fk,
                                           denominator_id=manufacturer_fk,
                                           result=brand_res, numerator_result=brand_num, denominator_result=brand_den,
                                           score=brand_res,
                                           identifier_result="{}_SOS_brand_{}".format(calculation_type, str(brand_fk)))
            # brand-category level sos
            brand_categories = set(self.parser.filter_df(conditions=filters,
                                                         data_frame_to_filter=sos_df)['category_fk'])
            for category_fk in brand_categories:
                cat_den = self.parser.filter_df(conditions={'category_fk': category_fk},
                                                data_frame_to_filter=sos_df)[calculation_param].sum()
                filters['category_fk'] = category_fk
                category_df = self.parser.filter_df(conditions=filters, data_frame_to_filter=sos_df)
                cat_num = category_df[calculation_param].sum()
                if cat_num == 0:
                    continue
                cat_res, cat_num, cat_den = self.calculate_sos_res(cat_num, cat_den)
                self.common.write_to_db_result(fk=brand_category_kpi_fk, numerator_id=brand_fk,
                                               context_id=manufacturer_fk,
                                               denominator_id=category_fk, result=cat_res, numerator_result=cat_num,
                                               should_enter=True, denominator_result=cat_den, score=cat_res,
                                               identifier_parent="{}_SOS_brand_{}".format(calculation_type,
                                                                                          str(brand_fk)),
                                               identifier_result="{}_SOS_cat_{}_brand_{}".format(calculation_type,
                                                                                                 str(category_fk),
                                                                                                 str(brand_fk)))
                product_fks = set(self.parser.filter_df(conditions=filters, data_frame_to_filter=sos_df)['product_fk'])
                for sku in product_fks:
                    filters['product_fk'] = sku
                    product_df = self.parser.filter_df(conditions=filters, data_frame_to_filter=sos_df)
                    sku_num = product_df[calculation_param].sum()
                    if sku_num == 0:
                        continue
                    sku_result, sku_num, sku_den = self.calculate_sos_res(sku_num, cat_num)
                    self.common.write_to_db_result(fk=sku_kpi_fk, numerator_id=sku, denominator_id=brand_fk,
                                                   result=sku_result, numerator_result=sku_num, should_enter=True,
                                                   denominator_result=cat_num, score=sku_num,
                                                   context_id=category_fk, weight=manufacturer_fk,
                                                   identifier_parent="{}_SOS_cat_{}_brand_{}".format(calculation_type,
                                                                                                     str(category_fk),
                                                                                                     str(brand_fk)))
                del filters['product_fk']
            del filters['category_fk']
