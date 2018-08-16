
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
from KPIUtils_v2.DB.Common import Common
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert


import json

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from _mysql_exceptions import ProgrammingError
from datetime import datetime

__author__ = 'Yasmin'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
STATIC_ATOMIC = 'static.atomic_kpi'

# IN DB
MANUFACTUR = 'Manufacturer'
BRAND = 'Brand'
SUB_BRAND = 'Sub Brand'
SUB_CATEGORY = 'Sub Category'
SUBSEGMENT_KPI = 'Subsegment'
SUBSEGMENT_SET = 'Purina- Subsegment'
PRICE_SET = 'Purina- Price'
PRICE_KPI = 'Price Class'  # to be written as it is on the database

# In SCIF
SCIF_SUBSEGMENT = 'Nestle_Purina_Subsegment'  # to be written as it is on the database
SCIF_SUB_CATEOGRY = 'sub_category'
SCIF_SUB_BRAND = 'Nestle_Purina_Subbrand'
SCIF_PRICE = 'Nestle_Purina_Price_Class'
LINEAR_SIZE = u'gross_len_add_stack'
# gross_len_ign_stack
# gross_len_split_stack
# gross_len_add_stack
PURINA_KPI = [SUBSEGMENT_KPI, MANUFACTUR, BRAND]


PURINA_SETS = [SUBSEGMENT_SET, PRICE_SET]

class PURINAToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

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
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.session_fk = self.session_info['pk'].values[0]
        self.kpi_results_queries = []
        self.kpi_static_queries = []

    def calculate_purina(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        if not self.is_session_purina():
            return
        # Update all new static KPIs
        self.create_new_static_kpi()

        self.kpi_static_data = self.common.get_kpi_static_data(refresh=True)

        self.update_kpi_score()
        self.run_data_collecting()
        self.common.commit_results_data()

    def update_kpi_score(self):
        # Only to see results in join :(
        for set_name in PURINA_SETS:
            for kpi in PURINA_KPI:
                kpi_fk = self.get_kpi_fk_by_kpi_name(kpi, self.LEVEL2, set_name=set_name)
                self.common.old_write_to_db_result(kpi_fk, 1, self.LEVEL2)
        for kpi in [SUB_CATEGORY]:
            kpi_fk = self.get_kpi_fk_by_kpi_name(kpi, self.LEVEL2, set_name=SUBSEGMENT_SET)
            self.common.old_write_to_db_result(kpi_fk, 1, self.LEVEL2)
        for kpi in [PRICE_KPI]:
            kpi_fk = self.get_kpi_fk_by_kpi_name(kpi, self.LEVEL2, set_name=PRICE_SET)
            self.common.old_write_to_db_result(kpi_fk, 1, self.LEVEL2)

    def run_data_collecting(self):
        """
        This function run the man calculation of linear sos with sub category out of subsegment
        or price out of subsegment
        :param price_kpi:
        :return:
        """

        data = self.scif.dropna(subset=[SCIF_SUBSEGMENT, LINEAR_SIZE])
        if data.empty:
            Log.info("No relevant purina's products were found in session.")
            return

        subseg_name_list = data[SCIF_SUBSEGMENT].unique()
        for subseg in subseg_name_list:
            by_subseg = data.loc[data[SCIF_SUBSEGMENT] == subseg]
            subseg_ft = self.cm_to_ft(sum(by_subseg[LINEAR_SIZE]))
            atomic_fk = self.get_kpi_fk_by_kpi_name(subseg, self.LEVEL3, father=SUBSEGMENT_KPI, set_name=SUBSEGMENT_SET)
            self.common.old_write_to_db_result(fk=atomic_fk, level=self.LEVEL3, score=subseg_ft)
            atomic_fk = self.get_kpi_fk_by_kpi_name(subseg, self.LEVEL3, father=SUBSEGMENT_KPI, set_name=PRICE_SET)
            self.common.old_write_to_db_result(fk=atomic_fk, level=self.LEVEL3, score=subseg_ft)

            man = by_subseg['manufacturer_name'].unique()
            for mf in man:
                by_MF = by_subseg.loc[data['manufacturer_name'] == mf]
                manufacturer_ft = self.cm_to_ft(sum(by_MF[LINEAR_SIZE]))
                if manufacturer_ft:
                    atomic_fk = self.get_kpi_fk_by_kpi_name(mf, self.LEVEL3, father=MANUFACTUR, set_name=SUBSEGMENT_SET)
                    self.common.old_write_to_db_result(fk=atomic_fk, level=self.LEVEL3, score=manufacturer_ft,
                                                       result=subseg)
                    atomic_fk = self.get_kpi_fk_by_kpi_name(mf, self.LEVEL3, father=MANUFACTUR, set_name=PRICE_SET)
                    self.common.old_write_to_db_result(fk=atomic_fk, level=self.LEVEL3, score=manufacturer_ft,
                                                       result=subseg)

                brands = by_MF['brand_name'].unique()
                for brand in brands:
                    by_brand = by_MF.loc[data['brand_name'] == brand]
                    brand_ft = self.cm_to_ft(sum(by_brand[LINEAR_SIZE]))
                    atomic_fk = self.get_kpi_fk_by_kpi_name(brand, self.LEVEL3, father=BRAND, set_name=SUBSEGMENT_SET)
                    try:
                        self.common.old_write_to_db_result(fk=atomic_fk, level=self.LEVEL3, score=brand_ft,
                                                       result=subseg, result_2=mf)
                        atomic_fk = self.get_kpi_fk_by_kpi_name(brand, self.LEVEL3, father=BRAND, set_name=PRICE_SET)
                        self.common.old_write_to_db_result(fk=atomic_fk, level=self.LEVEL3, score=brand_ft,
                                                           result=subseg, result_2=mf)
                    except:
                        print ''

                    sub_cats = by_brand[SCIF_SUB_CATEOGRY].unique()
                    for sub_cat in sub_cats:
                        by_sub_cat = by_brand.loc[data[SCIF_SUB_CATEOGRY] == sub_cat]
                        sub_cat_ft = self.cm_to_ft(sum(by_sub_cat[LINEAR_SIZE]))
                        # write to db under sub category atomic kpi score with brand name in results
                        if sub_cat_ft:
                            atomic_fk = self.get_kpi_fk_by_kpi_name(sub_cat, self.LEVEL3, father=SUB_CATEGORY,
                                                                    set_name=SUBSEGMENT_SET)
                            self.common.old_write_to_db_result(fk=atomic_fk, level=self.LEVEL3, score=sub_cat_ft,
                                                               result=subseg, result_2=mf, result_3=brand)
                    by_brand = by_brand.dropna(subset=[SCIF_PRICE])
                    prices = by_brand[SCIF_PRICE].unique()
                    for price_class in prices:
                        by_prices = by_brand.loc[data[SCIF_PRICE] == price_class]
                        price_ft = self.cm_to_ft(sum(by_prices[LINEAR_SIZE]))
                        # write to db under price atomic kpi score with brand name in results
                        if price_ft:
                            atomic_fk = self.get_kpi_fk_by_kpi_name(price_class, self.LEVEL3, father=PRICE_KPI,
                                                                    set_name=PRICE_SET)
                            self.common.old_write_to_db_result(fk=atomic_fk, level=self.LEVEL3, score=price_ft,
                                                               result=subseg, result_2=mf, result_3=brand)
    @staticmethod
    def cm_to_ft(cm):
        return cm / 30.48

    def get_labels(self):
        query = """select pk, labels, ean_code
        from static_new.product
        """
        labels = pd.read_sql_query(query, self.rds_conn.db)
        return labels

    def get_kpi_fk_by_kpi_name(self, kpi_name, kpi_level, father=None, logic_father=None, set_name=None):
        if kpi_level == self.LEVEL1:
            column_key = 'kpi_set_fk'
            column_value = 'kpi_set_name'
            father_value = 'kpi_set_name'


        elif kpi_level == self.LEVEL2:
            column_key = 'kpi_fk'
            column_value = 'kpi_name'
            father_value = 'kpi_set_name'


        elif kpi_level == self.LEVEL3:
            column_key = 'atomic_kpi_fk'
            column_value = 'atomic_kpi_name'
            father_value = 'kpi_name'

        else:
            raise ValueError('invalid level')

        try:
            relevant = self.kpi_static_data[self.kpi_static_data[column_value] == kpi_name]
            if father:
                relevant = relevant[relevant[father_value] == father]
            if set_name:
                relevant = relevant[relevant['kpi_set_name'] == set_name]

            return relevant[column_key].values[0]

        except IndexError:
            Log.info('Kpi name: {}, isn\'t equal to any kpi name in static table'.format(kpi_name))
            return None

    def create_new_static_kpi(self):
        #  This functions takes all brands, sub categories and manufacturer in session.
        #  The function adds them to database in case they are new.
        brands = self.get_all_brands()
        sub_cats = self.get_all_sub_categories()
        manufacturer = self.get_all_manufacturers()
        subsegs = self.get_all_subsegments()
        prices = self.get_all_price_classes()

        new_brands = self.scif.loc[~self.scif['brand_name'].isin(brands)]['brand_name'].unique()
        new_manufacturer = self.scif.loc[~self.scif['manufacturer_name'].isin(manufacturer)][
            'manufacturer_name'].unique()
        new_sub_cat = self.scif.loc[(~self.scif[SCIF_SUB_CATEOGRY].isin(sub_cats)) &
                                       (~pd.isnull(self.scif[SCIF_SUB_CATEOGRY]))][SCIF_SUB_CATEOGRY].unique()

        new_subsegs = self.scif.loc[(~self.scif[SCIF_SUBSEGMENT].isin(subsegs)) &
                                       (~pd.isnull(self.scif[SCIF_SUBSEGMENT]))][SCIF_SUBSEGMENT].unique()
        new_prices = self.scif.loc[(~self.scif[SCIF_PRICE].isin(prices)) &
                                       (~pd.isnull(self.scif[SCIF_PRICE]))][SCIF_PRICE].unique()

        for set_name in PURINA_SETS:
            self.save_static_atomics(BRAND, new_brands, set_name)
            self.save_static_atomics(MANUFACTUR, new_manufacturer, set_name)
            self.save_static_atomics(SUBSEGMENT_KPI, new_subsegs, set_name)

            if set_name == SUBSEGMENT_SET:
                self.save_static_atomics(SUB_CATEGORY, new_sub_cat, SUBSEGMENT_SET)
            else:
                self.save_static_atomics(PRICE_KPI, new_prices, PRICE_SET)

        self.commit_static_data()

    def get_all_brands(self):
        return self.kpi_static_data.loc[self.kpi_static_data['kpi_name'] == BRAND]['atomic_kpi_name']

    def get_all_sub_categories(self):
        return self.kpi_static_data.loc[self.kpi_static_data['kpi_name'] == SUB_CATEGORY]['atomic_kpi_name']

    def get_all_manufacturers(self):
        return self.kpi_static_data.loc[self.kpi_static_data['kpi_name'] == MANUFACTUR]['atomic_kpi_name']

    def get_all_subsegments(self):
        return self.kpi_static_data.loc[self.kpi_static_data['kpi_name'] == SUBSEGMENT_KPI]['atomic_kpi_name']

    def get_all_price_classes(self):
        return self.kpi_static_data.loc[self.kpi_static_data['kpi_name'] == PRICE_KPI]['atomic_kpi_name']

    def save_static_atomics(self, kpi_name, atomics, set_name):
        kpi_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_name'] == kpi_name) &
                                          (self.kpi_static_data['kpi_set_name'] == set_name)]['kpi_fk'].values[0]
        for current in atomics:
            current = current.replace("'", "''")
            query = """
               INSERT INTO {0} (`kpi_fk`, `name`, `description`, `display_text`,`presentation_order`, `display`)
               VALUES ('{1}', '{2}', '{3}', '{4}', '{5}', '{6}');""".format(STATIC_ATOMIC,
                                                                            kpi_fk, current, current, current, 1, 'Y')

            self.kpi_static_queries.append(query)

    def commit_static_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        self.rds_conn.disconnect_rds()
        self.rds_conn.connect_rds()
        # ProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = self.rds_conn.db.cursor()
        for query in self.kpi_static_queries:
            try:
                cur.execute(query)
            except Exception as e:
                Log.info('query {} could not be executed.'.format(query))
        self.rds_conn.db.commit()

        self.rds_conn.disconnect_rds()

    def is_session_purina(self):
        # This function checks is the session is of Purina project by its category and that it is a successful visit.
        session_data = self.get_session_category_data()
        session_data = session_data.loc[(session_data['category_fk'] == 13) &
                                        (session_data['resolution_fk'] == 1) &
                                        (session_data['exclude_status_fk'] == 1)]
        if not session_data.empty:
            return True
        return False

    def get_session_category_data(self):
        local_con = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        query = """select category_fk, resolution_fk, exclude_status_fk from probedata.session_category
                where session_fk = {}""".format(self.session_fk)
        data = pd.read_sql_query(query, local_con.db)
        return data



