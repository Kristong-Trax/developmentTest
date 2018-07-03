import os
import pandas as pd
import numpy as np
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log

# from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nissand'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class CCITSceneToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data',
                                 'KPI_Templates.xlsx')
    CCIT_MANU = 'HBC Italia'
    OCCUPANCY_SHEET = 'occupancy_target'
    FULFILLMENT_SKUS_SHEET = 'SKU_Points'
    FULFILLMENT_TARGETS_SHEET = 'Max_Uniqe_Facings'
    POINTS_SUFFIX = '-Points'
    SKU_TYPE_SUFFIX = '-SKU Type'
    FULFILLMENT_SKU_RESULT_HEADER = ['ean_code', 'points', 'type', 'dist']
    TYPE_KPI_MAP = {'Non-core': 'Non_core_fulfillment_score', 'Core': 'Core_fulfillment_score'}
    FULFILLMENT_TYPE_RESULT_HEADER = ['total_points', 'type', 'num_of_skus', 'sku_point']
    OUT_TYPE = 'Out'

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
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
        self.store_type = self.data_provider.store_type
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.occupancy_template = pd.read_excel(self.TEMPLATE_PATH, sheetname=self.OCCUPANCY_SHEET)
        self.fulfillment_targets = pd.read_excel(self.TEMPLATE_PATH, sheetname=self.FULFILLMENT_TARGETS_SHEET)
        self.fulfillment_template = pd.read_excel(self.TEMPLATE_PATH, sheetname=self.FULFILLMENT_SKUS_SHEET)
        self.missing_products = []

    def get_manufacturer_fk(self, manu):
        return self.all_products[self.all_products['manufacturer_name'] ==
                                 manu]['manufacturer_fk'].drop_duplicates().values[0]

    def get_filtered_df(self, df, filters):
        for key, value in filters.items():
            try:
                df = df[df[key] == value]
            except KeyError:
                Log.warning('Field {} is not in the Data Frame'.format(key))
                continue
        return df

    def occupancy_calculation(self, identifier_parent):
        """
        This function calculates the KPI results.
        """
        numerator_filters = {
            'manufacturer_name': self.CCIT_MANU
        }
        filtered_products = self.get_filtered_df(self.products, numerator_filters)['product_fk']
        numerator_res = len(self.match_product_in_scene[self.match_product_in_scene['product_fk'].isin(filtered_products)])
        denominator_res = len(self.match_product_in_scene)
        result = np.divide(float(numerator_res), float(denominator_res))*100
        target_share = self.occupancy_template['target'].values[0]*100
        target_score = self.occupancy_template['points'].values[0]
        score = target_score if result >= target_share else 0
        kpi_fk_share = self.common.get_kpi_fk_by_kpi_type('occupancy_share')
        kpi_fk_score = self.common.get_kpi_fk_by_kpi_type('occupancy_score')
        manu_fk = self.get_manufacturer_fk(self.CCIT_MANU)
        template_fk = self.templates['template_fk'].drop_duplicates().values[0]
        self.common.write_to_db_result(fk=kpi_fk_share, numerator_id=manu_fk, numerator_result=numerator_res,
                                       result=round(result, 0), denominator_id=template_fk,
                                       denominator_result=denominator_res, score=score, target=target_share,
                                       by_scene=True)
        self.common.write_to_db_result(fk=kpi_fk_score, numerator_id=manu_fk, numerator_result=score, result=score,
                                       score=score, target=target_score, identifier_parent=identifier_parent,
                                       by_scene=True, should_enter=True)
        return score

    def fulfillment_sku_calculation(self):
        relevant_store_info = pd.DataFrame(columns=self.FULFILLMENT_SKU_RESULT_HEADER)
        relevant_store_info[['ean_code', 'points', 'type']] = self.fulfillment_template[self.fulfillment_template[
            self.store_type + self.SKU_TYPE_SUFFIX] != self.OUT_TYPE][['ean_code', self.store_type + self.POINTS_SUFFIX,
                                                                       self.store_type + self.SKU_TYPE_SUFFIX]].\
            drop_duplicates()

        relevant_products = self.match_product_in_scene['product_fk'].drop_duplicates().values
        Log.info("Self products type: {}".format(type(self.products['product_fk'].values[0])))
        Log.info("Relevant products type: {}".format(type(relevant_products[0])))
        self.products['product_fk'] = self.products.product_fk.astype(np.int64)
        products = self.products[self.products['product_fk'].isin(relevant_products)][
                                              'product_ean_code'].dropna()

        Log.info("Prodcuts df {}".format(type(products.values[0])))

        products_in_scene = pd.to_numeric(products).values
        Log.info("products_in_scene {}".format(type(products_in_scene[0])))
        relevant_store_info['dist'] = 0
        relevant_store_info.loc[relevant_store_info['ean_code'].isin(products_in_scene), 'dist'] = 1
        kpi_fk = self.common.get_kpi_fk_by_kpi_type('fulfillment_SKUs_score')
        identifier_parent = None
        for row in relevant_store_info.itertuples():
            product_fk = self.all_products[self.all_products['product_ean_code'] == str(row.ean_code)]['product_fk'].\
                drop_duplicates()
            if product_fk.empty:
                self.missing_products.append(row.ean_code)
                continue
            else:
                product_fk = product_fk.values[0]
            res = row.dist * row.points
            identifier_parent = self.common.get_dictionary(kpi_fk=
                                                           self.common.get_kpi_fk_by_kpi_type(
                                                               self.TYPE_KPI_MAP[row.type]))
            identifier_parent['scene_fk'] = self.scene_info['scene_fk'].values[0]
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, numerator_result=res, result=res,
                                           score=res, identifier_parent=identifier_parent, by_scene=True,
                                           should_enter=True)
        Log.info('Missing Products EAN_Code: {}'.format(self.missing_products))
        return relevant_store_info

    def fulfillment_type_calculation(self, sku_res):
        type_aggrigation = pd.DataFrame(columns=self.FULFILLMENT_TYPE_RESULT_HEADER)
        type_aggrigation['type'] = self.TYPE_KPI_MAP.keys()
        for sku_type in self.TYPE_KPI_MAP.keys():
            type_aggrigation.loc[type_aggrigation['type'] == sku_type, 'total_points'] = \
                sku_res[(sku_res['type'] == sku_type) & (sku_res['dist'])]['points'].sum()
            type_aggrigation.loc[type_aggrigation['type'] == sku_type, 'num_of_skus'] = \
                len(sku_res[(sku_res['type'] == sku_type) & (sku_res['dist'])])
            type_aggrigation.loc[type_aggrigation['type'] == sku_type, 'sku_point'] = \
                sku_res[sku_res['type'] == sku_type]['points'].drop_duplicates().values[0]
        identifier_parent = self.common.get_dictionary(
            kpi_fk=self.common.get_kpi_fk_by_kpi_type('fulfillment_scene_score'),
            template_fk=self.templates['template_fk'].drop_duplicates().values[0])
        manu_fk = self.get_manufacturer_fk(self.CCIT_MANU)
        for row in type_aggrigation.itertuples():
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.TYPE_KPI_MAP[row.type])
            identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
            identifier_result['scene_fk'] = self.scene_info['scene_fk'].values[0]
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=manu_fk, numerator_result=row.num_of_skus, result=row.num_of_skus,
                                           score=row.total_points, identifier_parent=identifier_parent,
                                           identifier_result=identifier_result, by_scene=True, should_enter=True)
        return type_aggrigation, identifier_parent

    def fulfillment_scene_calculation(self, type_results, identifier_result, identifier_parent):
        max_unique_skus = self.fulfillment_targets[self.fulfillment_targets['Template Name'] == self.templates[
            'template_name'].drop_duplicates().values[0]][self.store_type].values[0]
        core_skus = type_results[type_results['type'] == 'Core']['num_of_skus'].values[0]
        non_core_skus = type_results[type_results['type'] == 'Non-core']['num_of_skus'].values[0]
        if core_skus > max_unique_skus:
            result = max_unique_skus * \
                     type_results[type_results['type'] == 'Core']['sku_point'].drop_duplicates().values[0]
        elif core_skus + non_core_skus > max_unique_skus:
            non_core_diff = max_unique_skus - core_skus
            result = type_results[type_results['type'] == 'Core']['total_points'].drop_duplicates().values[0] + \
                     (non_core_diff * type_results[type_results['type'] == 'Non-core'][
                         'sku_point'].drop_duplicates().values[0])
        else:
            result = type_results['total_points'].sum()
        kpi_fk = self.common.get_kpi_fk_by_kpi_type('fulfillment_scene_score')
        template_fk = self.templates['template_fk'].drop_duplicates().values[0]
        target = max_unique_skus * type_results[type_results['type'] == 'Core']['sku_point'].drop_duplicates().values[0]
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=template_fk, numerator_result=result, result=result,
                                       score=result, target=target, identifier_parent=identifier_parent,
                                       identifier_result=identifier_result, by_scene=True, should_enter=True)
        return result

    def fulfillment_calculation(self, identifier_parent):
        sku_results = self.fulfillment_sku_calculation()
        type_results, sku_identifier_parent = self.fulfillment_type_calculation(sku_results)
        scene_results = self.fulfillment_scene_calculation(type_results, sku_identifier_parent, identifier_parent)
        return scene_results

    def scene_score(self):
        identifier_result = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type('scene_score'),
                                               template_fk=self.templates['template_fk'].drop_duplicates().values[0])
        occupancy_score = self.occupancy_calculation(identifier_result)
        fulfillment_score = self.fulfillment_calculation(identifier_result)
        result = occupancy_score + fulfillment_score
        kpi_fk = self.common.get_kpi_fk_by_kpi_type('scene_score')
        template_fk = self.templates['template_fk'].drop_duplicates().values[0]
        identifier_parent = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type('store_score'))
        identifier_parent['session_fk'] = self.session_info['pk'].values[0]
        identifier_parent['store_fk'] = self.session_info['store_fk'].values[0]
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=template_fk, numerator_result=result, result=result,
                                       score=result, identifier_result=identifier_result, by_scene=True,
                                       should_enter=True)
        return
