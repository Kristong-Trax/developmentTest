
import os
import json
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime

from Projects.MOLSONCOORSHR_SAND.Utils.ParseTemplates import parse_template
from Projects.MOLSONCOORSHR_SAND.Utils.Fetcher import MOLSONCOORSHR_SANDQueries


__author__ = 'sergey'

KPIS_TEMPLATE_NAME = 'MOLSONCOORS_HR_KPIs.xlsx'
KPIS_TEMPLATE_SHEET = 'KPIs'

NUMERATOR = 'numerator'
DENOMINATOR = 'denominator'

LINEAR_SOS_VS_TARGET = 'linear sos vs target'
FACINGS_SOS_VS_TARGET = 'facings sos vs target'
FACINGS_VS_TARGET = 'facings vs target'
DISTRIBUTION = 'distribution'
GROUP = 'group'
SUM_OF_SCORES = 'sum of scores'
CONDITIONAL_PROPORTIONAL = 'conditional proportional'
WEIGHTED_AVERAGE = 'weighted average'
WEIGHTED_SCORE = 'weighted score'

SOS_MANUFACTURER_CATEGORY = 'SOS_manufacturer_category'

MANUFACTURER_NAME = 'manufacturer_name'
MANUFACTURER = 'manufacturer'
MANUFACTURER_FK = 'manufacturer_fk'
CATEGORY = 'category'
CATEGORY_FK = 'category_fk'
LOCATION_TYPE = 'location_type'


class MOLSONCOORSHR_SANDToolBox:

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
        self.current_date = datetime.now()
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.own_manufacturer_id = int(self.data_provider[Data.OWN_MANUFACTURER][self.data_provider[Data.OWN_MANUFACTURER]['param_name'] == 'manufacturer_id']['param_value'].tolist()[0])
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.toolbox = GENERALToolBox(data_provider)
        self.assortment = Assortment(self.data_provider, self.output, common=self.common)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

        self.template_path = self.get_template_path()
        self.template_data = self.get_template_data()
        self.sos_store_policies = self.get_sos_store_policies(self.visit_date.strftime('%Y-%m-%d'))
        self.result_values = self.get_result_values()

        self.scores = pd.DataFrame()

    def get_sos_store_policies(self, visit_date):
        query = MOLSONCOORSHR_SANDQueries.get_sos_store_policies(visit_date)
        store_policies = pd.read_sql_query(query, self.rds_conn.db)
        return store_policies

    def get_result_values(self):
        query = MOLSONCOORSHR_SANDQueries.get_result_values()
        result_values = pd.read_sql_query(query, self.rds_conn.db)
        return result_values

    def main_calculation(self):
        """
        This function starts the KPI results calculation.
        """
        if not self.template_data or KPIS_TEMPLATE_SHEET not in self.template_data:
            Log.error('KPIs template sheet is empty or not found')
            return

        self.kpis_calculation()
        self.common.commit_results_data()

    def kpis_calculation(self, kpi_group=''):
        """
        This is a recursive function.
        The function calculates each level KPI cascading from the highest level to the lowest.
        """
        total_score = total_potential_score = total_calculated = 0
        kpis = self.template_data['KPIs'][self.template_data['KPIs']['KPI Group'] == kpi_group]
        for index, kpi in kpis.iterrows():

            child_kpi_group = kpi['Child KPI Group']
            kpi_type = kpi['KPI Type'].lower()
            score_function = kpi['Score Function'].lower()

            if not child_kpi_group:
                if kpi_type in [LINEAR_SOS_VS_TARGET, FACINGS_SOS_VS_TARGET]:
                    score, potential_score, calculated = self.calculate_sos_vs_target(kpi)
                elif kpi_type in [FACINGS_VS_TARGET, DISTRIBUTION]:
                    score, potential_score, calculated = self.calculate_assortment_vs_target(kpi)
                else:
                    Log.error("KPI of type '{}' is not supported".format(kpi_type))
                    score = potential_score = calculated = 0
            else:
                score, potential_score, calculated = self.kpis_calculation(child_kpi_group)

            if score_function in [WEIGHTED_SCORE, SUM_OF_SCORES]:
                total_score += score
                total_potential_score += potential_score
                total_calculated += calculated
            else:
                total_score += 0
                total_potential_score += 0
                total_calculated += 0

            if child_kpi_group and calculated:
                if kpi['KPI name Eng'] == 'Store Score':
                    kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi['KPI name Eng'])
                    parent_fk = self.common.get_kpi_fk_by_kpi_type(kpi['KPI Group']) if kpi['KPI Group'] else 0
                    numerator_id = self.own_manufacturer_id
                    denominator_id = self.store_id
                    identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
                    identifier_parent = self.common.get_dictionary(kpi_fk=parent_fk)
                    self.common.write_to_db_result(fk=kpi_fk,
                                                   numerator_id=numerator_id,
                                                   numerator_result=0,
                                                   denominator_id=denominator_id,
                                                   denominator_result=0,
                                                   result=score,
                                                   score=score,
                                                   weight=potential_score,
                                                   target=potential_score,
                                                   identifier_result=identifier_result,
                                                   identifier_parent=identifier_parent,
                                                   should_enter=True
                                                   )
                else:
                    kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi['KPI name Eng'])
                    parent_fk = self.common.get_kpi_fk_by_kpi_type(kpi['KPI Group']) if kpi['KPI Group'] else 0
                    numerator_id = self.own_manufacturer_id
                    denominator_id = self.store_id
                    identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
                    identifier_parent = self.common.get_dictionary(kpi_fk=parent_fk)
                    self.common.write_to_db_result(fk=kpi_fk,
                                                   numerator_id=numerator_id,
                                                   numerator_result=0,
                                                   denominator_id=denominator_id,
                                                   denominator_result=0,
                                                   result=score,
                                                   score=score,
                                                   weight=potential_score,
                                                   target=potential_score,
                                                   identifier_result=identifier_result,
                                                   identifier_parent=identifier_parent,
                                                   should_enter=True
                                                   )

        return total_score, total_potential_score, total_calculated

    @kpi_runtime()
    def calculate_assortment_vs_target(self, kpi):
        """
        The function filters only the relevant scenes by Location Type and calculates the Assortment scores
        according to rules set in the target.
        :return:
        """
        lvl3_result = self.calculate_assortment_vs_target_lvl3(kpi)
        for row in lvl3_result.itertuples():
            numerator_id = row.product_fk
            numerator_result = row.distributed if kpi['KPI Type'] == 'Distribution' else row.facings
            denominator_id = self.store_id
            denominator_result = row.target
            # denominator_result_after_actions = 0 if row.target < row.facings else row.target - row.facings
            if kpi['KPI Type'] == 'Distribution':
                if row.result_distributed:
                    result = self.result_values[(self.result_values['result_type'] == 'PRESENCE') &
                                                (self.result_values['result_value'] == 'DISTRIBUTED')]['result_value_fk'].tolist()[0]
                    score = 100
                else:
                    result = self.result_values[(self.result_values['result_type'] == 'PRESENCE') &
                                                (self.result_values['result_value'] == 'OOS')]['result_value_fk'].tolist()[0]
                    score = 0
            else:
                result = row.result_facings
                score = round(result*100, 0)
            identifier_details = self.common.get_dictionary(kpi_fk=row.kpi_fk_lvl3)
            identifier_kpi = self.common.get_dictionary(kpi_fk=row.kpi_fk_lvl2)
            self.common.write_to_db_result(fk=row.kpi_fk_lvl3,
                                           numerator_id=numerator_id,
                                           numerator_result=numerator_result,
                                           denominator_id=denominator_id,
                                           denominator_result=denominator_result,
                                           # denominator_result_after_actions=denominator_result_after_actions,
                                           result=result,
                                           score=score,
                                           identifier_result=identifier_details,
                                           identifier_parent=identifier_kpi,
                                           should_enter=True
                                           )

        score = potential_score = 0
        if not lvl3_result.empty:
            lvl2_result = self.calculate_assortment_vs_target_lvl2(lvl3_result)
            for row in lvl2_result.itertuples():
                numerator_id = self.own_manufacturer_id
                numerator_result = row.distributed if kpi['KPI Type'] == 'Distribution' else row.facings
                denominator_id = self.store_id
                denominator_result = row.target
                result = row.result_distributed if kpi['KPI Type'] == 'Distribution' else row.result_facings
                score += self.score_function(result*100, kpi)
                potential_score += round(float(kpi['Weight'])*100, 0)
                identifier_kpi = self.common.get_dictionary(kpi_fk=row.kpi_fk_lvl2)
                identifier_parent = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type(kpi['KPI Group']))
                self.common.write_to_db_result(fk=row.kpi_fk_lvl2,
                                               numerator_id=numerator_id,
                                               numerator_result=numerator_result,
                                               denominator_id=denominator_id,
                                               denominator_result=denominator_result,
                                               result=score,
                                               score=score,
                                               weight=potential_score,
                                               target=potential_score,
                                               identifier_result=identifier_kpi,
                                               identifier_parent=identifier_parent,
                                               should_enter=True
                                               )
        if len(lvl3_result) > 0:
            calculated = 1
        else:
            calculated = 0

        return score, potential_score, calculated

    def calculate_assortment_vs_target_lvl3(self, kpi):
        location_types = kpi['Location Type'].split(', ')
        kpi_fk_lvl3 = self.common.get_kpi_fk_by_kpi_type(kpi['KPI name Eng'] + ' - SKU')
        kpi_fk_lvl2 = self.common.get_kpi_fk_by_kpi_type(kpi['KPI name Eng'])

        assortment_result = self.assortment.get_lvl3_relevant_ass()
        if assortment_result.empty:
            return assortment_result
        assortment_result = assortment_result[(assortment_result['kpi_fk_lvl3'] == kpi_fk_lvl3) &
                                              (assortment_result['kpi_fk_lvl2'] == kpi_fk_lvl2)]
        if assortment_result.empty:
            return assortment_result

        assortment_result['target'] = assortment_result.apply(lambda x: json.loads(x['additional_attributes']).get('Target'), axis=1)
        assortment_result['target'] = assortment_result['target'].fillna(0)
        assortment_result = assortment_result[assortment_result['target'] > 0]

        assortment_result['weight'] = assortment_result.apply(lambda x: json.loads(x['additional_attributes']).get('Weight'), axis=1)
        assortment_result['weight'] = assortment_result['weight'].fillna(0)
        assortment_total_weights = assortment_result[['assortment_fk', 'weight']].groupby('assortment_fk').agg({'weight': 'sum'}).reset_index()
        assortment_result = assortment_result.merge(assortment_total_weights, how='left', left_on='assortment_fk', right_on='assortment_fk', suffixes=['', '_total'])

        facings = 'facings_ign_stack' if kpi['Ignore Stacking'] else 'facings'

        products_in_session = self.scif[(self.scif[facings] > 0) & (self.scif['location_type'].isin(location_types))][['product_fk', 'facings']]\
            .groupby('product_fk').agg({'facings': 'sum'}).reset_index()
        lvl3_result = assortment_result.merge(products_in_session, how='left', left_on='product_fk', right_on='product_fk')
        lvl3_result['facings'] = lvl3_result['facings'].fillna(0)
        lvl3_result['distributed'] = lvl3_result.apply(lambda x: 1 if x['facings'] else 0, axis=1)

        lvl3_result['result_facings'] = lvl3_result.apply(lambda x: self.assortment_vs_target_result(x, 'facings'), axis=1)
        lvl3_result['result_distributed'] = lvl3_result.apply(lambda x: self.assortment_vs_target_result(x, 'distributed'), axis=1)

        return lvl3_result

    def calculate_assortment_vs_target_lvl2(self, lvl3_result):
        lvl2_result = lvl3_result.groupby(['kpi_fk_lvl2', self.assortment.ASSORTMENT_FK, self.assortment.ASSORTMENT_GROUP_FK])\
            .agg({'facings': 'sum', 'distributed': 'sum', 'target': 'sum', 'result_facings': 'sum', 'result_distributed': 'sum'}).reset_index()
        return lvl2_result

    @staticmethod
    def assortment_vs_target_result(x, y):
        if x[y] < x['target']:
            return round(x[y] / float(x['target']) * float(x['weight']) / x['weight_total'], 5)
        else:
            return round(1 * float(x['weight']) / x['weight_total'], 5)

    @kpi_runtime()
    def calculate_sos_vs_target(self, kpi):
        """
        The function filters only the relevant scenes by Location Type and calculates the linear SOS and
        the facing SOS according to Manufacturer and Category set in the target.
         :return:
        """
        location_type = kpi['Location Type']
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(SOS_MANUFACTURER_CATEGORY + ('_' + location_type if location_type else ''))

        sos_store_policies = self.sos_store_policies[self.sos_store_policies['kpi_fk'] == str(kpi_fk)]

        sos_store_policy = None
        store_policy_passed = 0
        for index, policy in sos_store_policies.iterrows():
            sos_store_policy = policy
            store_policy = json.loads(policy['store_policy'])
            store_policy_passed = 1
            for key in store_policy.keys():
                if key in self.store_info.columns.tolist():
                    if self.store_info[key][0] in store_policy[key]:
                        continue
                    else:
                        store_policy_passed = 0
                        break
                else:
                    Log.error("Store Policy attribute is not found: '{}'").format(key)
                    store_policy_passed = 0
                    break
            if store_policy_passed:
                break

        score = potential_score = 0
        if store_policy_passed:

            general_filters = {LOCATION_TYPE: location_type}
            sos_policy = json.loads(sos_store_policy['sos_policy'])
            numerator_sos_filters = {MANUFACTURER_NAME: sos_policy[NUMERATOR][MANUFACTURER], CATEGORY: sos_policy[DENOMINATOR][CATEGORY]}
            denominator_sos_filters = {CATEGORY: sos_policy[DENOMINATOR][CATEGORY]}

            numerator_id = self.all_products.loc[self.all_products[MANUFACTURER_NAME] == sos_policy[NUMERATOR][MANUFACTURER]][MANUFACTURER + '_fk'].values[0]
            denominator_id = self.all_products.loc[self.all_products[CATEGORY] == sos_policy[DENOMINATOR][CATEGORY]][CATEGORY + '_fk'].values[0]

            ignore_stacking = kpi['Ignore Stacking'] if kpi['Ignore Stacking'] else 0

            numer_facings, numer_linear = self.calculate_share_space(ignore_stacking=ignore_stacking, **dict(numerator_sos_filters, **general_filters))
            denom_facings, denom_linear = self.calculate_share_space(ignore_stacking=ignore_stacking, **dict(denominator_sos_filters, **general_filters))

            if kpi['KPI Type'].lower() == LINEAR_SOS_VS_TARGET:
                numerator_result = round(numer_linear, 0)
                denominator_result = round(denom_linear, 0)
                result = numer_linear / float(denom_linear) if denom_linear else 0
            elif kpi['KPI Type'].lower() == FACINGS_SOS_VS_TARGET:
                numerator_result = numer_facings
                denominator_result = denom_facings
                result = numer_facings / float(denom_facings) if denom_facings else 0
            else:
                Log.error("KPI Type is invalid: '{}'").format(kpi['KPI Type'])
                numerator_result = denominator_result = result = 0

            if sos_store_policy['target']:
                sos_target = round(float(sos_store_policy['target'])*100, 0)
            else:
                Log.error("SOS target is not set for Store ID {}").format(self.store_id)
                sos_target = 0

            result_vs_target = result/(float(sos_target)/100)*100 if sos_target else 0
            score = self.score_function(result_vs_target, kpi)
            potential_score = round(float(kpi['Weight'])*100, 0)

            identifier_kpi = self.common.get_dictionary(kpi_fk=kpi_fk)
            identifier_parent = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type(kpi['KPI Group']))
            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=numerator_id,
                                           numerator_result=numerator_result,
                                           denominator_id=denominator_id,
                                           denominator_result=denominator_result,
                                           result=result,
                                           score=score,
                                           weight=potential_score,
                                           target=sos_target,
                                           identifier_result=identifier_kpi,
                                           identifier_parent=identifier_parent,
                                           should_enter=True
                                           )

        else:
            Log.warning("Store Policy is not found for Store ID {}".format(self.store_id))

        return score, potential_score, store_policy_passed

    def calculate_share_space(self, ignore_stacking=1, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :param ignore_stacking: 1 is to ignore stacking.
        :return: The total number of facings and the shelf width (in mm) according to the filters.
        """
        filtered_scif = self.scif[self.toolbox.get_filter_condition(self.scif, **filters)]
        if ignore_stacking:
            sum_of_facings = filtered_scif['facings_ign_stack'].sum()
            space_length = filtered_scif['net_len_ign_stack'].sum()
        else:
            sum_of_facings = filtered_scif['facings'].sum()
            space_length = filtered_scif['net_len_ign_stack'].sum()

        return sum_of_facings, space_length

    @staticmethod
    def get_template_path():
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', KPIS_TEMPLATE_NAME)

    def get_template_data(self):
        template_data = {}
        try:
            sheet_names = pd.ExcelFile(self.template_path).sheet_names
            for sheet in sheet_names:
                template_data[sheet] = parse_template(self.template_path, sheet, lower_headers_row_index=0)
        except IOError as e:
            Log.error('Template {} does not exist. {}'.format(KPIS_TEMPLATE_NAME, repr(e)))
        return template_data

    @staticmethod
    def score_function(score, kpi):
        weight = float(kpi['Weight']) if kpi['Weight'] else 1
        score_function = kpi['Score Function'].lower()
        l_threshold = float(kpi['Lower Threshold'])*100 if kpi['Lower Threshold'] else 0
        h_threshold = float(kpi['Higher Threshold'])*100 if kpi['Higher Threshold'] else 100

        if score < l_threshold:
            score = 0
        elif score >= h_threshold:
            score = 100

        if score_function in [WEIGHTED_SCORE]:
            score = round(score*weight, 0)
        else:
            score = round(score, 0)

        return score
