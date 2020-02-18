import os
import json

import pandas as pd
import numpy as np

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

# assortment KPIs
KPI_TYPE_COL = 'type'
# Codes
OOS_CODE = 1
PRESENT_CODE = 2
EXTRA_CODE = 3
# Assortment KPI Names
DST_MAN_BY_STORE_PERC = 'DST_MAN_BY_STORE_PERC'
OOS_MAN_BY_STORE_PERC = 'OOS_MAN_BY_STORE_PERC'
PRODUCT_PRESENCE_BY_STORE_LIST = 'DST_MAN_BY_STORE_PERC - SKU'
OOS_PRODUCT_BY_STORE_LIST = 'OOS_MAN_BY_STORE_PERC - SKU'
#  # Category based Assortment KPIs
DST_MAN_BY_CATEGORY_PERC = 'DST_ALL_CATEGORY_PERC'
OOS_MAN_BY_CATEGORY_PERC = 'OOS_ALL_CATEGORY_PERC'
PRODUCT_PRESENCE_BY_CATEGORY_LIST = 'DST_ALL_CATEGORY_PERC - SKU'
OOS_MAN_BY_CATEGORY_LIST = 'OOS_ALL_CATEGORY_PERC - SKU'

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
# assortment KPI endss

__author__ = 'limorc'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

BAY_COUNT_SHEET = 'BayCountKPI'
BAY_COUNT_KPI = 'BAY_COUNT_BY_SCENE_TYPE_IN_WHOLE_STORE'


class CCAAUToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.own_manufacturer_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.template = self.data_provider.all_templates  # templates
        self.kpi_static_data = self.common.get_new_kpi_static_data()
        self.toolbox = GENERALToolBox(data_provider)
        kpi_path = os.path.dirname(os.path.realpath(__file__))
        base_file = os.path.basename(kpi_path)
        self.exclude_filters = pd.read_excel(os.path.join(kpi_path[:- len(base_file)], 'Data', 'template.xlsx'),
                                             sheetname="Exclude")
        self.Include_filters = pd.read_excel(os.path.join(kpi_path[:- len(base_file)], 'Data', 'template.xlsx'),
                                             sheetname="Include")
        self.bay_count_kpi = pd.read_excel(os.path.join(kpi_path[:- len(base_file)], 'Data', 'template.xlsx'),
                                           sheetname="BayCountKPI")

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        self.calculate_sos()
        self.common.commit_results_data_to_new_tables()
        self.calculate_assortment_kpis()

    def calculate_sos(self):

        """
            This function filtering Data frame - "scene item facts" by the parameters in the template.
            Sending the filtered data frames to linear Sos calculation and facing Sos calculation
            Writing the results to the new tables in DB

        """
        facing_kpi_fk = self.kpi_static_data[self.kpi_static_data['client_name'] == 'FACINGS_SOS_SCENE_TYPE_BY_MANUFACTURER']['pk'].iloc[0]
        linear_kpi_fk = self.kpi_static_data[self.kpi_static_data['client_name'] == 'LINEAR_SOS_SCENE_TYPE_BY_MANUFACTURER']['pk'].iloc[0]
        den_facing_exclude_template = self.exclude_filters[(self.exclude_filters['KPI'] == 'Share of Shelf by Facing') & (self.exclude_filters['apply on'] == 'Denominator')]
        den_linear_exclude_template = self.exclude_filters[(self.exclude_filters['KPI'] == 'Share of Shelf by Linear')& (self.exclude_filters['apply on'] == 'Denominator')]
        num_facing_exclude_template = self.exclude_filters[(self.exclude_filters['KPI'] == 'Share of Shelf by Facing') & (self.exclude_filters['apply on'] == 'Numerator')]
        num_linear_exclude_template = self.exclude_filters[(self.exclude_filters['KPI'] == 'Share of Shelf by Linear') & (self.exclude_filters['apply on'] == 'Numerator')]

        scene_templates = self.scif['template_fk'].unique().tolist()
        scene_manufactures = self.scif['manufacturer_fk'].unique().tolist()

        # exclude filters denominator
        den_general_facing_filters = self.create_dict_filters(den_facing_exclude_template, self.EXCLUDE_FILTER)
        den_general_linear_filters = self.create_dict_filters(den_linear_exclude_template, self.EXCLUDE_FILTER)

        # exclude filters numerator
        num_general_facing_filters = self.create_dict_filters(num_facing_exclude_template, self.EXCLUDE_FILTER)
        num_general_linear_filters = self.create_dict_filters(num_linear_exclude_template, self.EXCLUDE_FILTER)

        df_num_fac = self.filter_2_cond(self.scif, num_facing_exclude_template)
        df_num_lin = self.filter_2_cond(self.scif, num_linear_exclude_template)
        df_den_lin = self.filter_2_cond(self.scif, den_facing_exclude_template)
        df_den_fac = self.filter_2_cond(self.scif, den_linear_exclude_template)

        for template in scene_templates:

            for manufacture in scene_manufactures:

                sos_filters = {"template_fk": (template, self.INCLUDE_FILTER),
                               "manufacturer_fk": (manufacture, self.INCLUDE_FILTER)}
                tem_filters = {"template_fk": (template, self.INCLUDE_FILTER)}

                dict_num_facing = dict(
                    (k, v) for d in [sos_filters, num_general_facing_filters] for k, v in
                    d.items())
                numerator_facings = self.calculate_share_space(df_num_fac, dict_num_facing)[0]

                dict_num_linear = dict(
                    (k, v) for d in [sos_filters, num_general_linear_filters] for k, v in d.items())
                numerator_linear = self.calculate_share_space(df_num_lin, dict_num_linear)[1]

                dict_den_facing = dict((k, v) for d in [tem_filters, den_general_facing_filters] for k, v in d.items())
                denominator_facings = self.calculate_share_space(df_den_fac, dict_den_facing)[0]

                dict_den_linear = dict(
                    (k, v) for d in [tem_filters, den_general_linear_filters] for k, v in d.items())
                denominator_linear = self.calculate_share_space(df_den_lin, dict_den_linear)[1]

                score_facing = 0 if denominator_facings == 0 else (numerator_facings/denominator_facings)*100
                score_linear = 0 if denominator_linear == 0 else (numerator_linear / denominator_linear)*100

                self.common.write_to_db_result_new_tables(facing_kpi_fk,manufacture ,numerator_facings,score_facing,template,
                                                          denominator_facings ,score_facing)
                self.common.write_to_db_result_new_tables(linear_kpi_fk, manufacture, numerator_linear,score_linear, template,
                                                          denominator_linear,
                                                          score_linear)


    def create_dict_filters(self, template, parametr):
        """
               :param template : Template of the desired filtering to data frame
                      parameter : exclude /include
               :return: Dictionary of filters and parameter : exclude / include by demeaned
        """

        filters_dict = {}
        template_without_second = template[template['Param 2'].isnull()]

        for row in template_without_second.iterrows():
            filters_dict[row[1]['Param 1']] = (row[1]['Value 1'].split(','), parametr)

        return filters_dict

    def filter_2_cond(self,df,template):
        """
               :param template: Template of the desired filtering
                      df : Data frame
               :return: data frame filtered by entries in the template with 2 conditions
        """

        template_without_second = template[template['Param 2'].notnull()]

        if template_without_second is not None:
            for row in template_without_second.iterrows():
                df = df.loc[(~df[row[1]['Param 1']].isin(row[1]['Value 1'].split(',')))| (~df[row[1]['Param 2']].isin(row[1]['Value 2'].split(',')))]

        return df

    def calculate_share_space(self,df, filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total number of facings and the shelf width (in mm) according to the filters.
        """
        filtered_scif = df[self.toolbox.get_filter_condition(df,**filters)]
        sum_of_facings = filtered_scif['facings'].sum()
        space_length = filtered_scif['gross_len_add_stack'].sum()
        return sum_of_facings, space_length

    def calculate_bay_kpi(self):
        bay_kpi_sheet = self.bay_count_kpi
        kpi = self.kpi_static_data.loc[self.kpi_static_data['type'] == BAY_COUNT_KPI]
        if kpi.empty:
            Log.info("CCAAU Calculate KPI Name:{} not found in DB".format(BAY_COUNT_KPI))
        else:
            Log.info("CCAAU Calculate KPI Name:{} found in DB".format(BAY_COUNT_KPI))
            bay_kpi_row = bay_kpi_sheet[bay_kpi_sheet['KPI Name'] == BAY_COUNT_KPI]
            if not bay_kpi_row.empty:
                scene_types_to_consider = bay_kpi_row['Scene Type'].iloc[0]
                if scene_types_to_consider == '*':
                    # Consider all scene types
                    scene_types_to_consider = 'all'
                else:
                    scene_types_to_consider = [x.strip() for x in scene_types_to_consider.split(',')]
                mpis_with_scene = self.match_product_in_scene.merge(self.scene_info, how='left', on='scene_fk')
                mpis_with_scene_and_template = mpis_with_scene.merge(self.templates, how='left', on='template_fk')
                if scene_types_to_consider != 'all':
                    mpis_with_scene_and_template = mpis_with_scene_and_template[
                        mpis_with_scene_and_template['template_name'].isin(scene_types_to_consider)]
                mpis_template_group = mpis_with_scene_and_template.groupby('template_fk')
                for template_fk, template_data in mpis_template_group:
                    Log.info("Running for template ID {templ_id}".format(
                        templ_id=template_fk,
                    ))
                    total_bays_for_scene_type = 0
                    scene_group = template_data.groupby('scene_fk')
                    for scene_fk, scene_data in scene_group:
                        Log.info("KPI Name:{kpi} bay count is {bay_c} for scene ID {scene_id}".format(
                            kpi=BAY_COUNT_KPI,
                            bay_c=int(scene_data['bay_number'].max()),
                            scene_id=scene_fk,
                        ))
                        total_bays_for_scene_type += int(scene_data['bay_number'].max())
                    Log.info("KPI Name:{kpi} total bay count is {bay_c} for template ID {templ_id}".format(
                        kpi=BAY_COUNT_KPI,
                        bay_c=total_bays_for_scene_type,
                        templ_id=template_fk,
                    ))
                    self.common.write_to_db_result_new_tables(
                        fk=int(kpi['pk'].iloc[0]),
                        numerator_id=int(template_fk),
                        numerator_result=total_bays_for_scene_type,
                        denominator_id=int(self.store_id),
                        denominator_result=total_bays_for_scene_type,
                        result=total_bays_for_scene_type,
                    )

    def calculate_assortment_kpis(self):
        # get filter data starts
        kpi_include_exclude = self.kpi_template.parse(KPI_INC_EXC_SHEET)
        valid_scenes = kpi_include_exclude[
            kpi_include_exclude[KPI_NAME_COL] == ASSORTMENTS]['scene_types_to_include'].values[0]
        valid_categories = kpi_include_exclude[
            kpi_include_exclude[KPI_NAME_COL] == ASSORTMENTS]['categories_to_include'].values[0]
        # get filter data ends
        valid_scenes = [x.strip() for x in valid_scenes.split(',') if x]
        valid_categories = [x.strip() for x in valid_categories.split(',') if x]
        valid_scif = self.scif[(self.scif['template_name'].isin(valid_scenes)) &
                               (self.scif['category'].isin(valid_categories))]
        distribution_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == DST_MAN_BY_STORE_PERC)
                                                & (self.kpi_static_data['delete_time'].isnull())]
        oos_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == OOS_MAN_BY_STORE_PERC)
                                       & (self.kpi_static_data['delete_time'].isnull())]
        prod_presence_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == PRODUCT_PRESENCE_BY_STORE_LIST)
                                                 & (self.kpi_static_data['delete_time'].isnull())]
        oos_prod_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == OOS_PRODUCT_BY_STORE_LIST)
                                            & (self.kpi_static_data['delete_time'].isnull())]
        # Category based Assortments
        distribution_by_cat_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == DST_MAN_BY_CATEGORY_PERC)
                                                       & (self.kpi_static_data['delete_time'].isnull())]
        oos_by_cat_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == OOS_MAN_BY_CATEGORY_PERC)
                                              & (self.kpi_static_data['delete_time'].isnull())]
        prod_presence_by_cat_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] ==
                                                         PRODUCT_PRESENCE_BY_CATEGORY_LIST)
                                                        & (self.kpi_static_data['delete_time'].isnull())]
        oos_by_cat_prod_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == OOS_MAN_BY_CATEGORY_LIST)
                                                   & (self.kpi_static_data['delete_time'].isnull())]

        def __return_valid_store_policies(policy):
            valid_store = True
            policy_json = json.loads(policy)
            # special case where its only one assortment for all
            # that is there is only one key and it is is_active => Y
            if len(policy_json) == 1 and policy_json.get('is_active') == ['Y']:
                return valid_store

            store_json = json.loads(self.store_info.reset_index().to_json(orient='records'))[0]
            # map the necessary keys to those names knows
            for policy_value, store_info_value in POLICY_STORE_MAP.iteritems():
                if policy_value in policy_json:
                    policy_json[store_info_value] = policy_json.pop(policy_value)
            for key, values in policy_json.iteritems():
                if str(store_json.get(key, 'is_active')) in values:
                    continue
                else:
                    valid_store = False
                    break
            return valid_store

        policy_data = self.get_policies(distribution_kpi.iloc[0].pk)
        if policy_data.empty:
            Log.info("No Assortments Loaded.")
            return 0
        resp = policy_data['policy'].apply(__return_valid_store_policies)
        valid_policy_data = policy_data[resp]
        if valid_policy_data.empty:
            Log.info("No policy applicable for session {sess} and kpi {kpi}.".format(
                sess=self.session_uid,
                kpi=distribution_kpi.iloc[0].type))
            return 0
        # calculate and save the percentage values for distribution and oos
        self.calculate_and_save_distribution_and_oos(
            valid_scif=valid_scif,
            assortment_product_fks=valid_policy_data['product_fk'],
            distribution_kpi_fk=distribution_kpi.iloc[0].pk,
            oos_kpi_fk=oos_kpi.iloc[0].pk
        )
        # calculate and save prod presence and oos products
        self.calculate_and_save_prod_presence_and_oos_products(
            valid_scif=valid_scif,
            assortment_product_fks=valid_policy_data['product_fk'],
            prod_presence_kpi_fk=prod_presence_kpi.iloc[0].pk,
            oos_prod_kpi_fk=oos_prod_kpi.iloc[0].pk,
            distribution_kpi_name=DST_MAN_BY_STORE_PERC,
            oos_kpi_name=OOS_MAN_BY_STORE_PERC
        )
        # calculate and save the percentage values for distribution and oos
        self.calculate_and_save_distribution_and_oos_category(
            valid_scif=valid_scif,
            assortment_product_fks=valid_policy_data['product_fk'],
            distribution_kpi_fk=distribution_by_cat_kpi.iloc[0].pk,
            oos_kpi_fk=oos_by_cat_kpi.iloc[0].pk
        )
        # calculate and save prod presence and oos products
        self.calculate_and_save_prod_presence_and_oos_products_category(
            valid_scif=valid_scif,
            assortment_product_fks=valid_policy_data['product_fk'],
            prod_presence_kpi_fk=prod_presence_by_cat_kpi.iloc[0].pk,
            oos_prod_kpi_fk=oos_by_cat_prod_kpi.iloc[0].pk,
            distribution_kpi_name=DST_MAN_BY_CATEGORY_PERC,
            oos_kpi_name=OOS_MAN_BY_CATEGORY_PERC
        )

    def calculate_and_save_prod_presence_and_oos_products(self, valid_scif, assortment_product_fks,
                                                          prod_presence_kpi_fk, oos_prod_kpi_fk,
                                                          distribution_kpi_name, oos_kpi_name):
        # all assortment products are only in own manufacturers context;
        # but we have the products and hence no need to filter out denominator
        Log.info("Calculate product presence and OOS products for {}".format(self.project_name))
        total_products_in_scene = valid_scif["item_id"].unique()
        present_products = np.intersect1d(total_products_in_scene, assortment_product_fks)
        extra_products = np.setdiff1d(total_products_in_scene, present_products)
        oos_products = np.setdiff1d(assortment_product_fks, present_products)
        product_map = {
            OOS_CODE: oos_products,
            PRESENT_CODE: present_products,
            # EXTRA_CODE: extra_products
        }
        # save product presence; with distribution % kpi as parent
        for assortment_code, product_fks in product_map.iteritems():
            for each_fk in product_fks:
                self.common.write_to_db_result(fk=prod_presence_kpi_fk,
                                               numerator_id=each_fk,
                                               denominator_id=self.store_id,
                                               context_id=self.store_id,
                                               result=assortment_code,
                                               score=assortment_code,
                                               identifier_result=CODE_KPI_MAP.get(assortment_code),
                                               identifier_parent="{}_{}".format(distribution_kpi_name, self.store_id),
                                               should_enter=True
                                               )
            if assortment_code == OOS_CODE:
                # save OOS products; with OOS % kpi as parent
                for each_fk in product_fks:
                    self.common.write_to_db_result(fk=oos_prod_kpi_fk,
                                                   numerator_id=each_fk,
                                                   denominator_id=self.store_id,
                                                   context_id=self.store_id,
                                                   result=assortment_code,
                                                   score=assortment_code,
                                                   identifier_result=CODE_KPI_MAP.get(assortment_code),
                                                   identifier_parent="{}_{}".format(oos_kpi_name, self.store_id),
                                                   should_enter=True
                                                   )

    def calculate_and_save_distribution_and_oos(self, valid_scif, assortment_product_fks,
                                                distribution_kpi_fk, oos_kpi_fk):
        """Function to calculate distribution and OOS percentage.
        Saves distribution and oos percentage as values.
        """
        Log.info("Calculate distribution and OOS for {}".format(self.project_name))
        scene_products = pd.Series(valid_scif["item_id"].unique())
        total_products_in_assortment = len(assortment_product_fks)
        count_of_assortment_prod_in_scene = assortment_product_fks.isin(scene_products).sum()
        oos_count = total_products_in_assortment - count_of_assortment_prod_in_scene
        #  count of lion sku / all sku assortment count
        if not total_products_in_assortment:
            Log.info("No assortments applicable for session {sess}.".format(sess=self.session_uid))
            return 0
        distribution_perc = count_of_assortment_prod_in_scene / float(total_products_in_assortment) * 100
        oos_perc = 100 - distribution_perc
        self.common.write_to_db_result(fk=distribution_kpi_fk,
                                       numerator_id=self.own_manufacturer_fk,
                                       numerator_result=count_of_assortment_prod_in_scene,
                                       denominator_id=self.store_id,
                                       denominator_result=total_products_in_assortment,
                                       context_id=self.store_id,
                                       result=distribution_perc,
                                       score=distribution_perc,
                                       identifier_result="{}_{}".format(DST_MAN_BY_STORE_PERC, self.store_id),
                                       should_enter=True
                                       )
        self.common.write_to_db_result(fk=oos_kpi_fk,
                                       numerator_id=self.own_manufacturer_fk,
                                       numerator_result=oos_count,
                                       denominator_id=self.store_id,
                                       denominator_result=total_products_in_assortment,
                                       context_id=self.store_id,
                                       result=oos_perc,
                                       score=oos_perc,
                                       identifier_result="{}_{}".format(OOS_MAN_BY_STORE_PERC, self.store_id),
                                       should_enter=True
                                       )

    def calculate_and_save_prod_presence_and_oos_products_category(self, valid_scif, assortment_product_fks,
                                                                   prod_presence_kpi_fk, oos_prod_kpi_fk,
                                                                   distribution_kpi_name, oos_kpi_name):
        # all assortment products are only in own manufacturers context;
        # but we have the products and hence no need to filter out denominator
        Log.info("Calculate product presence and OOS products per Category for {}".format(self.project_name))
        scene_category_group = valid_scif.groupby('category_fk')
        for category_fk, each_scif_data in scene_category_group:
            total_products_in_scene_for_cat = each_scif_data["item_id"].unique()
            curr_category_products_in_assortment_df = self.all_products[
                (self.all_products.product_fk.isin(assortment_product_fks))
                & (self.all_products.category_fk == category_fk)]
            curr_category_products_in_assortment = curr_category_products_in_assortment_df['product_fk'].unique()
            present_products = np.intersect1d(total_products_in_scene_for_cat, curr_category_products_in_assortment)
            extra_products = np.setdiff1d(total_products_in_scene_for_cat, present_products)
            oos_products = np.setdiff1d(curr_category_products_in_assortment, present_products)
            product_map = {
                OOS_CODE: oos_products,
                PRESENT_CODE: present_products,
                # EXTRA_CODE: extra_products
            }
            # save product presence; with distribution % kpi as parent
            for assortment_code, product_fks in product_map.iteritems():
                for each_fk in product_fks:
                    self.common.write_to_db_result(fk=prod_presence_kpi_fk,
                                                   numerator_id=each_fk,
                                                   denominator_id=category_fk,
                                                   context_id=self.store_id,
                                                   result=assortment_code,
                                                   score=assortment_code,
                                                   identifier_result=CODE_KPI_MAP.get(assortment_code),
                                                   identifier_parent="{}_{}".format(distribution_kpi_name, category_fk),
                                                   should_enter=True
                                                   )
                if assortment_code == OOS_CODE:
                    # save OOS products; with OOS % kpi as parent
                    for each_fk in product_fks:
                        self.common.write_to_db_result(fk=oos_prod_kpi_fk,
                                                       numerator_id=each_fk,
                                                       denominator_id=category_fk,
                                                       context_id=self.store_id,
                                                       result=assortment_code,
                                                       score=assortment_code,
                                                       identifier_result=CODE_KPI_MAP.get(assortment_code),
                                                       identifier_parent="{}_{}".format(oos_kpi_name, category_fk),
                                                       should_enter=True
                                                       )

    def calculate_and_save_distribution_and_oos_category(self, valid_scif, assortment_product_fks,
                                                         distribution_kpi_fk, oos_kpi_fk):
        """Function to calculate distribution and OOS percentage by Category.
        Saves distribution and oos percentage as values.
        """
        Log.info("Calculate distribution and OOS per Category for {}".format(self.project_name))
        scene_category_group = valid_scif.groupby('category_fk')
        for category_fk, each_scif_data in scene_category_group:
            scene_products = pd.Series(each_scif_data["item_id"].unique())
            # find products in assortment belonging to categor_fk
            curr_category_products_in_assortment = len(self.all_products[
                (self.all_products.product_fk.isin(assortment_product_fks))
                & (self.all_products.category_fk == category_fk)])
            count_of_assortment_prod_in_scene = assortment_product_fks.isin(scene_products).sum()
            oos_count = curr_category_products_in_assortment - count_of_assortment_prod_in_scene
            #  count of lion sku / all sku assortment count
            if not curr_category_products_in_assortment:
                Log.info("No products from assortment with category: {cat} found in session {sess}.".format(
                    cat=category_fk,
                    sess=self.session_uid))
                distribution_perc = 0
                continue
            else:
                distribution_perc = count_of_assortment_prod_in_scene / float(curr_category_products_in_assortment) * 100
            oos_perc = 100 - distribution_perc
            self.common.write_to_db_result(fk=distribution_kpi_fk,
                                           numerator_id=self.own_manufacturer_fk,
                                           numerator_result=count_of_assortment_prod_in_scene,
                                           denominator_id=category_fk,
                                           denominator_result=curr_category_products_in_assortment,
                                           context_id=self.store_id,
                                           result=distribution_perc,
                                           score=distribution_perc,
                                           identifier_result="{}_{}".format(DST_MAN_BY_CATEGORY_PERC, category_fk),
                                           should_enter=True
                                           )
            self.common.write_to_db_result(fk=oos_kpi_fk,
                                           numerator_id=self.own_manufacturer_fk,
                                           numerator_result=oos_count,
                                           denominator_id=category_fk,
                                           denominator_result=curr_category_products_in_assortment,
                                           context_id=self.store_id,
                                           result=oos_perc,
                                           score=oos_perc,
                                           identifier_result="{}_{}".format(OOS_MAN_BY_CATEGORY_PERC, category_fk),
                                           should_enter=True
                                           )

    def get_policies(self, kpi_fk):
        query = """ select a.kpi_fk, p.policy_name, p.policy, atag.assortment_group_fk,
                        atp.assortment_fk, atp.product_fk, atp.start_date, atp.end_date
                    from pservice.assortment_to_product atp
                        join pservice.assortment_to_assortment_group atag on atp.assortment_fk = atag.assortment_fk
                        join pservice.assortment a on a.pk = atag.assortment_group_fk
                        join pservice.policy p on p.pk = a.store_policy_group_fk
                    where a.kpi_fk={kpi_fk};
                    """
        policies = pd.read_sql_query(query.format(kpi_fk=kpi_fk), self.rds_conn.db)
        return policies