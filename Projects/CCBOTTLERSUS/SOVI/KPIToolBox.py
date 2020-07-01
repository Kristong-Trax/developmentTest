from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from Projects.CCBOTTLERSUS.SOVI.Const import Const

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations


__author__ = 'Hunter & Nicolas'


class SOVIToolBox:

    def __init__(self, data_provider, output, common_v2):
        self.output = output
        self.data_provider = data_provider
        self.common = common_v2
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.region = self.store_info['region_name'].iloc[0]
        if self.region == Const.UNITED:
            self.manufacturer_attribute = 'United Deliver'
            self.manufacturer_value = 'Y'
        else:
            self.manufacturer_attribute = 'manufacturer_fk'
            self.manufacturer_value = Const.OWN_MANUFACTURER_FK
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.pseudo_pk = 0

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.sanitize_scif()
        self.calculate_total_sos()
        self.calculate_still_ssd()
        self.calculate_category()
        self.calculate_template_group()
        self.calculate_share_template_group()
        self.calculate_ssd_template_group()
        self.calculate_category_template_group()
        self.calculate_manufacturer_total_store()
        self.calculate_manufacturer_share_store()
        self.calculate_manufacturer_brandshare_store()
        self.calculate_manufacturer_productshare_store()
        self.calculate_manufacturer_template_group()
        self.calculate_store_template_group()
        self.calculate_manufacturer_share_template_group()
        self.calculate_brand_share_template_group()
        self.calculate_product_share_template_group()



    def calculate_total_sos(self):

        united_df = self.scif[self.scif[self.manufacturer_attribute] == self.manufacturer_value]

        numerator_result = united_df.facings.sum()
        denominator_result = self.scif.facings.sum()

        sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

        kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.TOTAL)
        parent_identifier = self.common.get_dictionary(kpi_type=Const.TOTAL)
        self.common.write_to_db_result(kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK, numerator_result=numerator_result,
                                       result=sos_value, identifier_result=parent_identifier,
                                       denominator_id=self.store_id, denominator_result=denominator_result)

    def calculate_still_ssd(self):
        for att4_value in Const.ATT4_VALUES:
            united_df = self.scif[(self.scif[self.manufacturer_attribute] == self.manufacturer_value) &
                                  (self.scif[Const.ATT4] == att4_value)]

            numerator_result = united_df.facings.sum()
            denominator_result = self.scif.facings.sum()

            sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

            kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.SSD)

            if numerator_result != 0:
                product_fk = united_df['product_fk'].iloc[0]

            parent_identifier = self.common.get_dictionary(kpi_type=Const.TOTAL)
            result_identifier = self.common.get_dictionary(kpi_type=Const.SHARE_SSD_TEMPLATE_GROUP, att4=att4_value)

            self.common.write_to_db_result(kpi_fk, numerator_id=product_fk,
                                           result=sos_value, identifier_result=result_identifier,
                                           identifier_parent=parent_identifier,
                                           denominator_id=self.store_id)

    def calculate_category(self):
        # Need to add att4 to contextid remove -1
        for category_fk in self.scif.category_fk.to_list():
            united_df = self.scif[(self.scif[self.manufacturer_attribute] == self.manufacturer_value) & (self.scif[Const.CATEGORY_FK] == category_fk )]

            numerator_result = united_df.facings.sum()
            denominator_result = self.scif.facings.sum()

            sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

            kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.CATEGORY)

            att4_value = -1
            if numerator_result != 0:
                att4_value = united_df['att4'].iloc[0]

            parent_identifier = self.common.get_dictionary(kpi_type=Const.SHARE_SSD_TEMPLATE_GROUP, att4=att4_value)

            self.common.write_to_db_result(kpi_fk, numerator_id=category_fk,
                                           numerator_result=numerator_result,
                                           result=sos_value,
                                           identifier_parent=parent_identifier,
                                           context_id=att4_value,
                                           denominator_id=self.store_id)

    def calculate_template_group(self):

        united_df = self.scif[(self.scif[self.manufacturer_attribute] == self.manufacturer_value) &
                              (self.scif[Const.template_group] == None)]

        numerator_result = united_df.facings.sum()
        denominator_result = self.scif.facings.sum()

        sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

        result_identifier = self.common.get_dictionary(kpi_type=Const.TEMPLATE_GROUP)
        parent_identifier = self.common.get_dictionary(kpi_type=Const.TOTAL)

        kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.TEMPLATE_GROUP)
        self.common.write_to_db_result(kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                       numerator_result=numerator_result,
                                       result=sos_value,
                                       denominator_id=self.store_id,
                                       denominator_result=denominator_result,
                                       identifier_result=result_identifier,
                                       identifier_parent=parent_identifier,
                                       )

    def calculate_share_template_group(self):
        for template_fk in self.scif.template_fk.to_list():
            united_df = self.scif[(self.scif[self.manufacturer_attribute] == self.manufacturer_value) &
                                  (self.scif[Const.TEMPLATE_FK] == template_fk)]

            numerator_result = united_df.facings.sum()
            denominator_result = self.scif.facings.sum()

            sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

            result_identifier = self.common.get_dictionary(kpi_type=Const.SHARE_TEMPLATE_GROUP)
            parent_identifier = self.common.get_dictionary(kpi_type=Const.TEMPLATE_GROUP)

            kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.TEMPLATE_GROUP)
            self.common.write_to_db_result(kpi_fk, numerator_id=template_fk,
                                           result=sos_value,
                                           denominator_id=self.store_id,
                                           identifier_result=result_identifier,
                                           identifier_parent=parent_identifier,
                                           )

    def calculate_ssd_template_group(self):
        # change att4 in number id from -1
        for template_fk in self.scif.template_fk.to_list():
            for att4_value in Const.ATT4_VALUES:
                united_df = self.scif[self.scif[self.manufacturer_attribute] == self.manufacturer_value]

                numerator_result = united_df.facings.sum()
                denominator_result = self.scif.facings.sum()

                sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

                result_identifier = self.common.get_dictionary(kpi_type=Const.SHARE_SSD_TEMPLATE_GROUP,
                                                               template_fk=template_fk, att4=att4_value)
                parent_identifier = self.common.get_dictionary(kpi_type=Const.SHARE_TEMPLATE_GROUP,
                                                               template_fk=template_fk)

                kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.SHARE_SSD_TEMPLATE_GROUP)
                self.common.write_to_db_result(kpi_fk, numerator_id=-1,
                                               result=sos_value,
                                               denominator_id=template_fk,
                                               identifier_result=result_identifier,
                                               identifier_parent=parent_identifier,
                                               )

    def calculate_category_template_group(self):
        # change att4 in context id from -1
        for template_fk in self.scif.template_fk.to_list():
            for category_fk in self.scif.category_fk.to_list():
                united_df = self.scif[self.scif[self.manufacturer_attribute] == self.manufacturer_value]

                numerator_result = united_df.facings.sum()
                denominator_result = self.scif.facings.sum()

                sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)
                att4_value = -1
                if numerator_result != 0:
                    att4_value = united_df['att4'].iloc[0]

                parent_identifier = self.common.get_dictionary(kpi_type=Const.SHARE_SSD_TEMPLATE_GROUP, template_fk=template_fk, att4=att4_value)

                kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.TEMPLATE_GROUP)
                self.common.write_to_db_result(kpi_fk, numerator_id=category_fk,
                                               result=sos_value,
                                               denominator_id=template_fk,
                                               context_id=-1,
                                               identifier_parent=parent_identifier,
                                               )

    def calculate_manufacturer_total_store(self):

        result_identifier = self.common.get_dictionary(kpi_type=Const.MANUFACTURER_TOTAL_STORE)
        parent_identifier = self.common.get_dictionary(kpi_type=Const.TOTAL)


        kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.TEMPLATE_GROUP)
        self.common.write_to_db_result(kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                       numerator_result=1,
                                       result=100,
                                       denominator_id=self.store_id,
                                       denominator_result=1,
                                       identifier_result=result_identifier,
                                       identifier_parent=parent_identifier,
                                       )
        

    def calculate_manufacturer_share_store(self):
        for manufacturer_fk in self.scif.manufacturer_fk.to_list():
            united_df = self.scif[(self.scif[self.manufacturer_attribute] == self.manufacturer_value) & (self.scif[Const.MANUFACTURER_FK] == manufacturer_fk)]

            numerator_result = united_df.facings.sum()
            denominator_result = self.scif.facings.sum()

            sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

            kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.MANUFACTURER_SHARE_STORE)


            result_identifier = self.common.get_dictionary(kpi_type=Const.MANUFACTURER_SHARE_STORE, manufacturer_fk=manufacturer_fk)
            parent_identifier = self.common.get_dictionary(kpi_type=Const.MANUFACTURER_TOTAL_STORE)

            self.common.write_to_db_result(kpi_fk, numerator_id=manufacturer_fk,
                                           numerator_result=numerator_result,
                                           denominator_result=denominator_result,
                                           result=sos_value,
                                           identifier_parent=parent_identifier,
                                           identifier_result=result_identifier,
                                           denominator_id=self.store_id)

    def calculate_manufacturer_brandshare_store(self):
        for manufacturer_fk in self.scif.manufacturer_fk.to_list():
            filtered_scif = self.scif[(self.scif[Const.MANUFACTURER_FK] == manufacturer_fk)]
            for brand_fk in filtered_scif.brand_fk.to_list():
                united_df = filtered_scif[(filtered_scif[Const.BRAND_FK] == brand_fk)]

                numerator_result = united_df.facings.sum()
                denominator_result = self.scif.facings.sum()

                sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

                kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.BRAND_SHARE_MANUFACTURER)

            result_identifier = self.common.get_dictionary(kpi_type=Const.BRAND_SHARE_MANUFACTURER, brand_fk=brand_fk)
            parent_identifier = self.common.get_dictionary(kpi_type=Const.MANUFACTURER_SHARE_STORE, manufacturer_fk=manufacturer_fk)

            self.common.write_to_db_result(kpi_fk, numerator_id=brand_fk,
                                           numerator_result=numerator_result,
                                           result=sos_value,
                                           identifier_parent=parent_identifier,
                                           identifier_result=result_identifier,
                                           context_id=manufacturer_fk,
                                           denominator_id=self.store_id)

    def calculate_manufacturer_productshare_store(self):
        for brand_fk in self.scif.brand_fk.to_list():
            filtered_scif = self.scif[(self.scif[Const.BRAND_FK] == brand_fk)]
            for product_fk in filtered_scif.product_fk.to_list():
                united_df = filtered_scif[(filtered_scif[Const.PRODUCT_FK] == product_fk)]

                numerator_result = united_df.facings.sum()
                denominator_result = self.scif.facings.sum()

                sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

                kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.CATEGORY)


            parent_identifier = self.common.get_dictionary(kpi_type=Const.BRAND_SHARE_MANUFACTURER, brand_fk=brand_fk)

            self.common.write_to_db_result(kpi_fk, numerator_id=product_fk,
                                           numerator_result=numerator_result,
                                           result=sos_value,
                                           identifier_parent=parent_identifier,
                                           context_id=brand_fk,
                                           denominator_id=self.store_id)

    def calculate_manufacturer_template_group(self):
        result_identifier = self.common.get_dictionary(kpi_type=Const.MANUFACTURER_TOTAL_STORE)
        parent_identifier = self.common.get_dictionary(kpi_type=Const.TOTAL)

        kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.MANUFACTURER_TEMPLATE_GROUP)
        self.common.write_to_db_result(kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                       numerator_result=1,
                                       result=100,
                                       denominator_id=self.store_id,
                                       denominator_result=1,
                                       identifier_result=result_identifier,
                                       identifier_parent=parent_identifier,
                                       )

    def calculate_store_template_group(self):
        for template_fk in self.scif.template_fk.to_list():
            united_df = self.scif[(self.scif[Const.TEMPLATE_FK] == template_fk)]

            numerator_result = united_df.facings.sum()
            denominator_result = self.scif.facings.sum()

            sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

            kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.MANUFACTURER_SHARE_STORE)


            result_identifier = self.common.get_dictionary(kpi_type=Const.SHARE_TEMPLATE_GROUP, template_fk=template_fk)
            parent_identifier = self.common.get_dictionary(kpi_type=Const.MANUFACTURER_TEMPLATE_GROUP)

            self.common.write_to_db_result(kpi_fk, numerator_id=template_fk,
                                           numerator_result=numerator_result,
                                           denominator_result=denominator_result,
                                           result=sos_value,
                                           identifier_parent=parent_identifier,
                                           identifier_result=result_identifier,
                                           denominator_id=self.store_id)

    def calculate_manufacturer_share_template_group(self):
        for manufacturer_fk in self.scif.manufacturer_fk.to_list():
            filtered_scif = self.scif[(self.scif[Const.MANUFACTURER_FK] == manufacturer_fk)]
            for template_fk in filtered_scif.brand_fk.to_list():
                united_df = filtered_scif[(filtered_scif[Const.TEMPLATE_FK] == template_fk)]

                numerator_result = united_df.facings.sum()
                denominator_result = self.scif.facings.sum()

                sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

                kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.MANUFACTURER_SHARE_STORE)

                parent_identifier = self.common.get_dictionary(kpi_type=Const.SHARE_TEMPLATE_GROUP, template_fk=template_fk)
            result_identifier = self.common.get_dictionary(kpi_type=Const.SHARE_TEMPLATE_GROUP,template_fk=template_fk, manufacturer_fk=manufacturer_fk)

            self.common.write_to_db_result(kpi_fk, numerator_id=manufacturer_fk,
                                           numerator_result=numerator_result,
                                           result=sos_value,
                                           identifier_parent=parent_identifier,
                                           identifier_result=result_identifier,
                                           context_id=manufacturer_fk,
                                           denominator_id=template_fk)

    def calculate_brand_share_template_group(self):
        for template_fk in self.scif.template_fk.to_list():
            filtered_scif = self.scif[(self.scif[Const.TEMPLATE_FK] == template_fk)]
            for brand_fk in filtered_scif.brand_fk.to_list():
                united_df = filtered_scif[(filtered_scif[Const.BRAND_FK] == brand_fk)]

                numerator_result = united_df.facings.sum()
                denominator_result = self.scif.facings.sum()

                sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

                kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.MANUFACTURER_SHARE_STORE)

                manufacturer_fk = self.scif['manufacturer_fk'][self.scif[Const.BRAND_FK== brand_fk]].iloc[0]
                parent_identifier = self.common.get_dictionary(kpi_type=Const.MANUFACTURER_TEMPLATE_GROUP, template_fk=template_fk, manufacturer_fk=manufacturer_fk)
            result_identifier = self.common.get_dictionary(kpi_type=Const.BRAND_TEMPLATE_GROUP,template_fk=template_fk, brand_fk=brand_fk)

            self.common.write_to_db_result(kpi_fk, numerator_id=brand_fk,
                                           numerator_result=numerator_result,
                                           result=sos_value,
                                           identifier_parent=parent_identifier,
                                           identifier_result=result_identifier,
                                           context_id=template_fk,
                                           denominator_id=template_fk)

    def calculate_product_share_template_group(self):
        for template_fk in self.scif.template_fk.to_list():
            filtered_scif = self.scif[(self.scif[Const.TEMPLATE_FK] == template_fk)]
            for product_fk in filtered_scif.product_fk.to_list():
                united_df = filtered_scif[(filtered_scif[Const.PRODUCT_FK] == product_fk)]

                numerator_result = united_df.facings.sum()
                denominator_result = self.scif.facings.sum()

                sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

                kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.BRAND_SHARE_MANUFACTURER)

                brand_fk = self.scif['brand_fk'][self.scif[Const.PRODUCT_FK== product_fk]].iloc[0]
                parent_identifier = self.common.get_dictionary(kpi_type=Const.BRAND_SHARE_MANUFACTURER, template_fk=template_fk, brand_fk=brand_fk)


            self.common.write_to_db_result(kpi_fk, numerator_id=product_fk,
                                           numerator_result=numerator_result,
                                           result=sos_value,
                                           identifier_parent=parent_identifier,
                                           context_id=brand_fk,
                                           denominator_id=template_fk)
    # def calculate_entire_store_sos(self):
    #     # general_filters = {}  # entire session/store visit
    #     # sos_filters = {self.manufacturer_attribute: 'Y'}
    #
    #     # this assumes that template groups where United does NOT have products should NOT be shown
    #     united_df = self.scif[self.scif[self.manufacturer_attribute] == self.manufacturer_value]
    #     template_group_list = united_df.template_group.unique().tolist()
    #
    #     numerator_result = united_df.facings.sum()
    #     denominator_result = self.scif.facings.sum()
    #
    #     sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)
    #     # print('Entire store: {}%'.format(sos_value))
    #
    #     own_pk = self.pseudo_pk
    #     kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.TOTAL)
    #     self.common.write_to_db_result(kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK, numerator_result=numerator_result,
    #                                    result=sos_value, identifier_result=own_pk,
    #                                    denominator_id=self.store_id, denominator_result=denominator_result)
    #
    #     for template_group in template_group_list:
    #         if template_group is not None:
    #             self.calculate_template_group_sos(template_group, own_pk)
    #
    # def calculate_template_group_sos(self, template_group, parent_pk):
    #     # general_filters = {}  # entire session/store visit
    #     # sos_filters = {self.manufacturer_attribute: self.manufacturer_value,
    #     #                'template_group': template_group}
    #
    #     template_group_df = self.scif[(self.scif[self.manufacturer_attribute] == self.manufacturer_value) &
    #                                   (self.scif['template_group'] == template_group)]
    #     att4_list = template_group_df.att4.unique().tolist()
    #     template_group_id = template_group_df.template_fk.unique()[0]
    #
    #     numerator_result = template_group_df.facings.sum()
    #     denominator_result = self.scif.facings.sum()
    #
    #     sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)
    #     # print('{}: {}%'.format(template_group, sos_value))
    #
    #     self.pseudo_pk += 1
    #     own_pk = self.pseudo_pk
    #     kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.TEMPLATE)
    #     self.common.write_to_db_result(kpi_fk, numerator_id=template_group_id, numerator_result=numerator_result,
    #                                    result=sos_value,
    #                                    denominator_id=self.store_id, denominator_result=denominator_result,
    #                                    identifier_parent=parent_pk, identifier_result=own_pk, should_enter=True)
    #
    #     for att4 in att4_list:
    #         if att4 is not None:
    #             self.calculate_att4_sos(template_group, att4, own_pk)
    #
    # def calculate_att4_sos(self, template_group, att4, parent_pk):
    #     # general_filters = {}  # entire session/store visit
    #     # sos_filters = {self.manufacturer_attribute: 'Y',
    #     #                'template_group': template_group,
    #     #                'att4': att4
    #     #                }
    #
    #     att4_df = self.scif[(self.scif[self.manufacturer_attribute] == self.manufacturer_value) &
    #                         (self.scif['template_group'] == template_group) &
    #                         (self.scif['att4'] == att4)]
    #     category_list = att4_df.category.unique().tolist()
    #     template_group_id = att4_df.template_fk.unique()[0]
    #     att4_id = Const.STILL_FK if att4 == 'Still' else Const.SSD_FK
    #
    #     numerator_result = att4_df.facings.sum()
    #     denominator_result = self.scif.facings.sum()
    #
    #     sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)
    #     # print('{} - {}: {}%'.format(template_group, att4, sos_value))
    #
    #     self.pseudo_pk += 1
    #     own_pk = self.pseudo_pk
    #     kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.ATT4)
    #     self.common.write_to_db_result(kpi_fk, numerator_id=att4_id, numerator_result=numerator_result,
    #                                    result=sos_value,
    #                                    denominator_id=template_group_id, denominator_result=denominator_result,
    #                                    identifier_parent=parent_pk, identifier_result=own_pk, should_enter=True)
    #
    #     for category in category_list:
    #         if category is not None:
    #             self.calculate_category_sos(template_group, att4, category, own_pk, template_group_id)
    #
    # def calculate_category_sos(self, template_group, att4, category, parent_pk, template_group_id):
    #     # general_filters = {}
    #     # sos_filters = {'United Deliver': 'Y',
    #     #                'template_group': template_group,
    #     #                'att4': att4,
    #     #                'category': category
    #     #                }
    #
    #     # we need to get manufacturers for the next KPI before applying United Deliver filter
    #     category_df = self.scif[(self.scif['template_group'] == template_group) &
    #                             (self.scif['att4'] == att4) &
    #                             (self.scif['category'] == category)]
    #     manufacturer_list = category_df.manufacturer_name.unique()
    #
    #     # we need to apply United Deliver filter to return the correct KPI result
    #     category_df = category_df[(category_df[self.manufacturer_attribute] == self.manufacturer_value)]
    #
    #     att4_id = Const.STILL_FK if att4 == 'Still' else Const.SSD_FK
    #     category_id = category_df.category_fk.unique()[0]
    #
    #     numerator_result = category_df.facings.sum()
    #     denominator_result = self.scif.facings.sum()
    #
    #     sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)
    #     # print('{} - {} - {}: {}%'.format(template_group, att4, category, sos_value))
    #
    #     self.pseudo_pk += 1
    #     own_pk = self.pseudo_pk
    #     kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.CATEGORY)
    #     self.common.write_to_db_result(kpi_fk, numerator_id=category_id, numerator_result=numerator_result,
    #                                    result=sos_value, context_id=template_group_id,
    #                                    denominator_id=att4_id, denominator_result=denominator_result,
    #                                    identifier_parent=parent_pk, identifier_result=own_pk, should_enter=True)
    #
    #     for manufacturer_name in manufacturer_list:
    #         self.calculate_manufacturer_sos(template_group, att4, category, manufacturer_name, own_pk)
    #
    # def calculate_manufacturer_sos(self, template_group, att4, category, manufacturer_name, parent_pk):
    #     general_filters = {
    #         'template_group': template_group,
    #         'att4': att4,
    #         'category': category
    #     }
    #     # sos_filters = {'manufacturer_name': manufacturer_name}
    #
    #     manufacturer_df = self.scif[(self.scif['template_group'] == template_group) &
    #                                 (self.scif['att4'] == att4) &
    #                                 (self.scif['category'] == category) &
    #                                 (self.scif['manufacturer_name'].str.encode("utf8") == manufacturer_name.encode(
    #                                     "utf-8"))]
    #
    #     brand_name_list = manufacturer_df.brand_name.unique().tolist()
    #     category_id = manufacturer_df.category_fk.unique()[0]
    #     manufacturer_id = manufacturer_df.manufacturer_fk.unique()[0]
    #
    #     numerator_result = manufacturer_df.facings.sum()
    #     denominator_result = self.apply_filters_to_df(self.scif, general_filters).facings.sum()
    #
    #     sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)
    #     # print('{} - {} - {} - {}: {}%'.format(template_group, att4, category, manufacturer_name, sos_value))
    #
    #     self.pseudo_pk += 1
    #     own_pk = self.pseudo_pk
    #     kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.MANUFACTURER)
    #     self.common.write_to_db_result(kpi_fk, numerator_id=manufacturer_id, numerator_result=numerator_result,
    #                                    result=sos_value,
    #                                    denominator_id=category_id, denominator_result=denominator_result,
    #                                    identifier_parent=parent_pk, identifier_result=own_pk, should_enter=True)
    #
    #     for brand_name in brand_name_list:
    #         if brand_name is not None:
    #             self.calculate_brand_sos(template_group, att4, category, manufacturer_name, brand_name, own_pk)
    #
    # def calculate_brand_sos(self, template_group, att4, category, manufacturer_name, brand_name, parent_pk):
    #     general_filters = {
    #         'template_group': template_group,
    #         'att4': att4,
    #         'category': category
    #     }
    #     # sos_filters = {'manufacturer_name': manufacturer_name,
    #     #                'brand_name': brand_name
    #     #                }
    #
    #     brand_df = self.scif[(self.scif['template_group'] == template_group) &
    #                          (self.scif['att4'] == att4) &
    #                          (self.scif['category'] == category) &
    #                          (self.scif['manufacturer_name'].str.encode("utf-8") == manufacturer_name.encode("utf-8")) &
    #                          (self.scif['brand_name'].str.encode("utf-8") == brand_name.encode("utf-8")) &
    #                          (self.scif['product_type'] != 'Empty')]
    #
    #     product_name_list = brand_df.product_name.unique().tolist()
    #     brand_id = brand_df.brand_fk.unique()[0]
    #     manufacturer_id = brand_df.manufacturer_fk.unique()[0]
    #
    #     numerator_result = brand_df.facings.sum()
    #     denominator_result = self.apply_filters_to_df(self.scif, general_filters).facings.sum()
    #
    #     sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)
    #     # print('{} - {} - {} - {} - {}: {}%'.format(template_group, att4, category, manufacturer_name,
    #     #                                            brand_name, sos_value))
    #
    #     self.pseudo_pk += 1
    #     own_pk = self.pseudo_pk
    #     kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.BRAND)
    #     self.common.write_to_db_result(kpi_fk, numerator_id=brand_id, numerator_result=numerator_result,
    #                                    result=sos_value,
    #                                    denominator_id=manufacturer_id, denominator_result=denominator_result,
    #                                    identifier_parent=parent_pk, identifier_result=own_pk, should_enter=True)
    #
    #     for product_name in product_name_list:
    #         if product_name is not None:
    #             self.calculate_product_name_sos(template_group, att4, category, manufacturer_name, brand_name,
    #                                             product_name, own_pk)
    #
    # def calculate_product_name_sos(self, template_group, att4, category, manufacturer_name, brand_name, product_name,
    #                                parent_pk):
    #     general_filters = {
    #         'template_group': template_group,
    #         'att4': att4,
    #         'category': category
    #     }
    #     # sos_filters = {'manufacturer_name': manufacturer_name,
    #     #                'brand_name': brand_name,
    #     #                'product_name': product_name
    #     #                }
    #
    #     product_df = self.scif[(self.scif['template_group'] == template_group) &
    #                            (self.scif['att4'] == att4) &
    #                            (self.scif['category'] == category) &
    #                            (self.scif['manufacturer_name'].str.encode("utf-8") == manufacturer_name.encode(
    #                                "utf-8")) &
    #                            (self.scif['brand_name'].str.encode("utf-8") == brand_name.encode("utf-8")) &
    #                            (self.scif['product_name'].str.encode("utf-8") == product_name.encode("utf-8"))]
    #
    #     product_id = product_df.product_fk.unique()[0]
    #     brand_id = product_df.brand_fk.unique()[0]
    #
    #     numerator_result = product_df.facings.sum()
    #     denominator_result = self.apply_filters_to_df(self.scif, general_filters).facings.sum()
    #
    #     sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)
    #
    #     self.pseudo_pk += 1
    #     # own_pk = self.pseudo_pk
    #     kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.PRODUCT)
    #     self.common.write_to_db_result(kpi_fk, numerator_id=product_id, numerator_result=numerator_result,
    #                                    result=sos_value,
    #                                    denominator_id=brand_id, denominator_result=denominator_result,
    #                                    identifier_parent=parent_pk, should_enter=True)
    #
    #     # print('{} - {} - {} - {} - {} - {}: {}%'.format(template_group, att4, category, manufacturer_name,
    #     #                                                 brand_name, product_name.encode('utf-8'), sos_value))

    def sanitize_scif(self):
        excluded_types = ['Empty', 'Irrelevant']
        self.scif = self.scif[~(self.scif['product_type'].isin(excluded_types)) &
                              ~(self.scif['brand_name'].isin(Const.EXCLUDED_BRANDS)) &
                              (self.scif['facings'] != 0)]

    @staticmethod
    def apply_filters_to_df(df, filters):
        for k, v in filters.iteritems():
            df = df[df[k] == v]
        return df

    @staticmethod
    def calculate_percentage_from_numerator_denominator(numerator_result, denominator_result):
        try:
            ratio = numerator_result / denominator_result
        except Exception as e:
            Log.error(e.message)
            ratio = 0
        if not isinstance(ratio, (float, int)):
            ratio = 0
        return round(ratio * 100, 2)

    def commit_results(self):
        pass
        # self.common_v2.commit_results_data()

    def commit_results_without_delete(self):
        self.common.commit_results_data_without_delete_version2()

    @staticmethod
    def get_dictionary(kpi_fk=None, brand_fk=None, sub_brand_fk=None, manufacturer_fk=None, brand_name=None,
                       sub_brand_name=None, product_fk=None, product_ean_code=None, att4=None, name=None, template_fk=None,
                       **kwargs):
        output_dict = {}
        if kpi_fk:
            output_dict["kpi_fk"] = kpi_fk
        if brand_fk:
            output_dict["brand_fk"] = brand_fk
        if template_fk:
            output_dict["template_fk"] = template_fk
        if sub_brand_fk:
            output_dict["sub_brand_fk"] = sub_brand_fk
        if manufacturer_fk:
            output_dict["manufacturer_fk"] = manufacturer_fk
        if brand_name:
            output_dict["brand_name"] = brand_name
        if sub_brand_name:
            output_dict["sub_brand_name"] = sub_brand_name
        if product_fk:
            output_dict["product_fk"] = product_fk
        if product_ean_code:
            output_dict["product_ean_code"] = product_ean_code
        if name:
            output_dict["name"] = name
        if name:
            output_dict["att4"] = att4
        for column in kwargs:
            output_dict[column] = kwargs[column]
        return output_dict
