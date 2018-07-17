
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Projects.CARREFOURAR.Utils.Fetcher import CARREFOUR_ARKPIFetcher

__author__ = 'ortal'
MAX_PARAMS = 3
CARREFOUR_INVENTORY = 'pservice.carrefour_inventory'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
MAX_SCORE = 100
PRODUCT_EAN_CODE = 'product_ean_code'
BRAND_NAME = 'brand_name'
ANY_KO_PRODUCT = 'Any KO Product in 1st Shoppable Position'


class CarrefourArKpiToolBox:
    def __init__(self, data_provider, output, set_name=None):
        self.data_provider = data_provider
        self.output = output
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = SessionInfo(data_provider)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        if not self.scif.empty:
            self.kpi_fetcher = CARREFOUR_ARKPIFetcher(self.project_name, self.scif, self.match_product_in_scene)
            self.store_number = self.kpi_fetcher.get_store_number()
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.set_name = set_name
        self.results_columns = ['session_id', 'product_fk', 'qty_in_store_stock', 'qty_in_dc',
                                'on_shelf', 'oos_but_in_store_stock', 'oos_but_in_dc', 'full_oos',
                                'sales_price']
        self.results = pd.DataFrame([], columns=self.results_columns)
        self.results_list = []

    def calculate_kpi(self, params):
        """
        :param params:
        :return:
        """
        self.products = self.products.loc[self.products['product_type'] == 'SKU']
        self.scif = self.scif.loc[self.scif['product_type'] == 'SKU']
        for index, p in params.iterrows():
            if p['EAN'] is not None:
                products = self.products.loc[self.products['product_ean_code'] == str(p['EAN'])]
                store = str(p['Qty at Store'])
                store = store.replace(',', '')
                store = float(store)
                store = int(store)
                dc = str(p['Qty at DC'])
                dc = dc.replace(',', '')
                dc = float(dc)
                dc = int(dc)
                if store < 0:
                    p['Qty at Store'] = 0
                else:
                    p['Qty at Store'] = store
                if dc < 0:
                    p['Qty at DC'] = 0
                else:
                    p['Qty at DC'] = dc
                if p['Sales Price'] < 0:
                    p['Sales Price'] = 0
                if not products.empty:
                    product_fk = products['product_fk'].values[0]
                    p_scif = self.scif[(self.scif['product_ean_code'] == str(p['EAN']))]
                    if not p_scif.empty:
                        if max(p_scif.get('dist_sc')) == 1:
                            on_shelf = 1
                            oos_but_in_store_stock = 0
                            oos_but_in_dc = 0
                            full_oos = 0
                        else:
                            on_shelf = 0
                            oos_but_in_store_stock = self.calculate_oos_but_in_store_stock(p)
                            oos_but_in_dc = self.calculate_oos_but_in_dc(p)
                            full_oos = self.calculate_full_oos(p)
                    else:
                        on_shelf = 0
                        oos_but_in_store_stock = self.calculate_oos_but_in_store_stock(p)
                        oos_but_in_dc = self.calculate_oos_but_in_dc(p)
                        full_oos = self.calculate_full_oos(p)
                    self.insert_to_results_df(p, on_shelf, oos_but_in_store_stock, oos_but_in_dc, full_oos, product_fk)

        self.results = pd.DataFrame(self.results_list, columns=self.results_columns)
        self.write_to_db_result()

    # @staticmethod
    # def calculate_on_shelf(p_scif):
    #     """
    #     :param p_skif:
    #     :param kpi_fk:
    #     :param children:
    #     :param params:
    #     :return:
    #     """
    #     result = 0
    #     if p_scif.empty:
    #         result = 0
    #     else:
    #         if max(p_scif['dist_sc']) == 1:
    #             result = 1
    #     return result

    @staticmethod
    def calculate_oos_but_in_store_stock(p):
        """
        :param p:
        :param p_scif:
        :param params:
        :return:
        """
        result = 0
        if int(float(p['Qty at Store'])) > 0:
                result = 1
        return result

    @staticmethod
    def calculate_oos_but_in_dc(p):
        """
        :param p_scif:
        :param p:
        :param params:
        :return:
        """
        result = 0
        if int(float(p['Qty at DC'])) > 0:
                    result = 1
        return result

    @staticmethod
    def calculate_full_oos(p):
        """
        :param p_scif:
        :param p:
        :param params:
        :return:
        """
        result = 0
        if int(float(p['Qty at Store'])) == 0 and int(float(p['Qty at DC'])) == 0:
            result = 1
        return result

    def write_to_db_result(self):
        """
        This function writes KPI results to old tables

        """
        query = self.kpi_fetcher.get_pk_to_delete(self.session_fk)
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        pk = pd.read_sql_query(query, self.rds_conn.db)
        if not pk.empty:
            pk_to_delete = tuple(pk['pk'].unique().tolist())
            delete_query = self.kpi_fetcher.get_delete_session_results(pk_to_delete)
            self.delete_results_data(delete_query)
        df_dict = self.results.to_dict()
        query = insert(df_dict, CARREFOUR_INVENTORY)
        self.insert_results_data(query)

    def delete_results_data(self, query):
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = self.rds_conn.db.cursor()
        cur.execute(query)

        self.rds_conn.db.commit()
        return

    def insert_results_data(self, query):
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = self.rds_conn.db.cursor()
        cur.execute(query)

        self.rds_conn.db.commit()
        return

    def insert_to_results_df(self, p,on_shelf, oos_but_in_store_stock, oos_but_in_dc, full_oos, product_fk):
        """
        This function creates a data frame with all attributes needed for saving in level 2 tables

        """
        self.results_list.append((self.session_fk, product_fk, p['Qty at Store'], p['Qty at DC']
                          ,on_shelf, oos_but_in_store_stock, oos_but_in_dc, full_oos, p['Sales Price']))

        return


