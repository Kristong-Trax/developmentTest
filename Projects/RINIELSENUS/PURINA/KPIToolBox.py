
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
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
CATEGORY = 'Category'
# SUBSEGMENT_KPI = 'Subsegment'
# SUBSEGMENT_SET = 'Purina- Subsegment'
# PRICE_SET = 'Purina- Price'
PRICE_KPI = 'Price Class'  # to be written as it is on the database

# In SCIF
# SCIF_SUBSEGMENT = 'Nestle_Purina_Subsegment'  # to be written as it is on the database
SCIF_SUB_CATEOGRY = 'Nestle_Purina_Sub-category'
# SCIF_SUB_BRAND = 'Nestle_Purina_Subbrand'
SCIF_PRICE = 'Nestle_Purina_Price_Class'
SCIF_CATEOGRY= 'Nestle_Purina_Category'
LINEAR_SIZE = u'gross_len_add_stack'
# gross_len_ign_stack
# gross_len_split_stack
# gross_len_add_stack
PURINA_KPI = [MANUFACTUR, BRAND, SUB_CATEGORY, CATEGORY, PRICE_KPI]
PET_FOOD_CATEGORY = 13
PURINA_SET = 'Purina'
OTHER = 'OTHER'

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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.session_fk = self.session_info['pk'].values[0]
        self.kpi_results_queries = []
        self.kpi_static_queries = []
        self.purina_scif = self.scif.loc[self.scif['category_fk'] == PET_FOOD_CATEGORY]

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

        for kpi in PURINA_KPI:
            kpi_fk = self.get_kpi_fk_by_kpi_name(kpi, self.LEVEL2, set_name=PURINA_SET)
            self.common.write_to_db_result(kpi_fk, self.LEVEL2, 1)

    @staticmethod
    def get_clean_vals(col):
        return list({x if x is not None else OTHER for x in col})


    def parse_sub_frame(self, df, col_name, subset, father):
        if subset == OTHER:
            df_out = df.loc[(pd.isnull(df[col_name])) | (df[col_name] == subset)]
        else:
            df_out = df[df[col_name] == subset]

        lin_ft = self.cm_to_ft(sum(df_out[LINEAR_SIZE]))

        kpi_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_name'] == father) &
                                          (self.kpi_static_data['kpi_set_name'] == PURINA_SET)]['kpi_fk'].values[0]
        atomic_fk = self.get_kpi_fk_by_kpi_name(subset, self.LEVEL3, father=father, set_name=PURINA_SET)

        return df_out, lin_ft, kpi_fk, atomic_fk


    def run_data_collecting(self):
        """
        This function run the man calculation of linear sos with sub category out of subsegment
        or price out of subsegment
        :param price_kpi:
        :return:
        """

        data = self.purina_scif.dropna(subset=[LINEAR_SIZE])

        if data.empty:
            Log.info("No relevant purina's products were found in session.")
            return

        # gets all category linear size

        data = data.loc[data['category_fk'] == PET_FOOD_CATEGORY]
        category_ft = self.cm_to_ft(sum(data[LINEAR_SIZE]))
        fk = self.get_kpi_fk_by_kpi_name(PURINA_SET, self.LEVEL1)
        self.common.write_to_db_result(fk, self.LEVEL1, category_ft)

        man = data['manufacturer_name'].unique()
        for mf in man:
            by_mf = data.loc[data['manufacturer_name'] == mf]
            manufacturer_ft = self.cm_to_ft(sum(by_mf[LINEAR_SIZE]))
            relevant_kpi_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_name'] == MANUFACTUR) &
                                          (self.kpi_static_data['kpi_set_name'] == PURINA_SET)]['kpi_fk'].values[0]
            atomic_fk = self.get_kpi_fk_by_kpi_name(mf, self.LEVEL3, father=MANUFACTUR, set_name=PURINA_SET)
            if atomic_fk:
                self.common.write_to_db_result(fk=atomic_fk, atomic_kpi_fk=atomic_fk, level=self.LEVEL3,
                                               score=manufacturer_ft, score_2=manufacturer_ft,
                                               session_uid=self.session_uid, store_fk=self.store_id,
                                               display_text=mf.replace("'","''"),
                                               visit_date=self.visit_date.isoformat(),
                                               calculation_time=datetime.utcnow().isoformat(),
                                               kps_name=PURINA_SET,
                                               kpi_fk=relevant_kpi_fk)
            else:
                print 'atomic cannot be saved for manufacturer {}'.format(mf)

            brands = by_mf['brand_name'].unique()
            for brand in brands:
                by_brand = by_mf.loc[by_mf['brand_name'] == brand]
                brand_ft = self.cm_to_ft(sum(by_brand[LINEAR_SIZE]))
                kpi_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_name'] == BRAND) &
                                                  (self.kpi_static_data['kpi_set_name'] == PURINA_SET)]['kpi_fk'].values[0]
                atomic_fk = self.get_kpi_fk_by_kpi_name(brand, self.LEVEL3, father=BRAND, set_name=PURINA_SET)
                if atomic_fk:
                    self.common.write_to_db_result(fk=atomic_fk, atomic_kpi_fk=atomic_fk, level=self.LEVEL3,
                                                   score=brand_ft, score_2=brand_ft, style=mf.replace("'","''"),
                                                   session_uid=self.session_uid, store_fk=self.store_id,
                                                   display_text=brand.replace("'","''"),
                                                   visit_date=self.visit_date.isoformat(),
                                                   calculation_time=datetime.utcnow().isoformat(),
                                                   kps_name=PURINA_SET,
                                                   kpi_fk=kpi_fk)
                else:
                    print 'atomic cannot be saved for brand {}'.format(brand)

                # categories = {cat if cat is not None else OTHER for cat in by_brand[SCIF_CATEOGRY]}
                categories = self.get_clean_vals(by_brand[SCIF_CATEOGRY])
                for cat in categories:
                    by_cat, cat_ft, kpi_fk, atomic_fk = self.parse_sub_frame(by_brand, SCIF_CATEOGRY, cat, CATEGORY)

                    # if cat == OTHER:
                    #     # cat = OTHER
                    #     by_cat = by_brand.loc[(pd.isnull(by_brand[SCIF_CATEOGRY])) | (by_brand[SCIF_CATEOGRY] == cat)]
                    #     cat_ft = self.cm_to_ft(sum(by_cat[LINEAR_SIZE]))
                    # else:
                    #     by_cat = by_brand.loc[by_brand[SCIF_CATEOGRY] == cat]
                    #     cat_ft = self.cm_to_ft(sum(by_cat[LINEAR_SIZE]))

                    # kpi_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_name'] == CATEGORY) &
                    #                       (self.kpi_static_data['kpi_set_name'] == PURINA_SET)]['kpi_fk'].values[0]
                    # atomic_fk = self.get_kpi_fk_by_kpi_name(cat, self.LEVEL3, father=CATEGORY, set_name=PURINA_SET)
                    if atomic_fk:
                        self.common.write_to_db_result(fk=atomic_fk, atomic_kpi_fk=atomic_fk, level=self.LEVEL3,
                                                       score=cat_ft,
                                                       score_2=cat_ft, style=mf.replace("'","''"),
                                                       result=brand.replace("'","''"),
                                                       session_uid=self.session_uid, store_fk=self.store_id,
                                                       display_text=cat.replace("'","''"),
                                                       visit_date=self.visit_date.isoformat(),
                                                       calculation_time=datetime.utcnow().isoformat(),
                                                       kps_name=PURINA_SET,
                                                       kpi_fk=kpi_fk)
                    else:
                        print 'atomic cannot be saved for category {}'.format(cat)

                    # sub_cats = by_cat[SCIF_SUB_CATEOGRY].unique()
                    sub_cats = self.get_clean_vals(by_cat[SCIF_SUB_CATEOGRY])
                    for sub_cat in sub_cats:
                        by_sub_cat, sub_cat_ft, kpi_fk, atomic_fk = self.parse_sub_frame(by_cat, SCIF_SUB_CATEOGRY, sub_cat, SUB_CATEGORY)

                        # if not sub_cat:
                        #     sub_cat = OTHER
                        #     by_sub_cat = by_cat.loc[pd.isnull(by_cat[SCIF_SUB_CATEOGRY])]
                        #     sub_cat_ft = self.cm_to_ft(sum(by_sub_cat[LINEAR_SIZE]))
                        # else:
                        #     by_sub_cat = by_cat.loc[by_cat[SCIF_SUB_CATEOGRY] == sub_cat]
                        #     sub_cat_ft = self.cm_to_ft(sum(by_sub_cat[LINEAR_SIZE]))
                        # # write to db under sub category atomic kpi score with brand name in results
                        #
                        # kpi_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_name'] == SUB_CATEGORY) &
                        #                                   (self.kpi_static_data['kpi_set_name'] == PURINA_SET)][
                        #                                                                             'kpi_fk'].values[0]
                        # atomic_fk = self.get_kpi_fk_by_kpi_name(sub_cat, self.LEVEL3, father=SUB_CATEGORY,
                        #                                             set_name=PURINA_SET)

                        if atomic_fk:
                            self.common.write_to_db_result(fk=atomic_fk, atomic_kpi_fk=atomic_fk, level=self.LEVEL3,
                                                           score=sub_cat_ft,
                                                           score_2=sub_cat_ft, style=mf.replace("'","''"),
                                                           result=brand.replace("'","''"),
                                                           result_2=cat.replace("'","''"),
                                                           session_uid=self.session_uid, store_fk=self.store_id,
                                                           display_text=sub_cat.replace("'","''"),
                                                           visit_date=self.visit_date.isoformat(),
                                                           calculation_time=datetime.utcnow().isoformat(),
                                                           kps_name=PURINA_SET,
                                                           kpi_fk=kpi_fk)
                        else:
                            print 'atomic cannot be saved for sub category {}'.format(sub_cat)

                        # prices = by_sub_cat[SCIF_PRICE].unique()
                        prices = self.get_clean_vals(by_sub_cat[SCIF_PRICE])
                        for price_class in prices:
                            by_prices, price_ft, kpi_fk, atomic_fk = self.parse_sub_frame(by_sub_cat, SCIF_PRICE, price_class, PRICE_KPI)

                            # if not price_class:
                            #     price_class = OTHER
                            #     by_prices = by_sub_cat.loc[pd.isnull(by_sub_cat[SCIF_PRICE])]
                            #     price_ft = self.cm_to_ft(sum(by_prices[LINEAR_SIZE]))
                            # else:
                            #     by_prices = by_sub_cat.loc[by_sub_cat[SCIF_PRICE] == price_class]
                            #     price_ft = self.cm_to_ft(sum(by_prices[LINEAR_SIZE]))
                            # kpi_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_name'] == PRICE_KPI) &
                            #                                   (self.kpi_static_data['kpi_set_name'] == PURINA_SET)][
                            #                                                                         'kpi_fk'].values[0]
                            # atomic_fk = self.get_kpi_fk_by_kpi_name(price_class, self.LEVEL3, father=PRICE_KPI,
                            #                                         set_name=PURINA_SET)

                            if atomic_fk:
                                self.common.write_to_db_result(fk=atomic_fk, atomic_kpi_fk=atomic_fk, level=self.LEVEL3,
                                                               score=price_ft,
                                                               score_2=price_ft, style=mf.replace("'","''"),
                                                               result=brand.replace("'","''"),
                                                               result_2=cat.replace("'","''"),
                                                               result_3=sub_cat.replace("'","''"),
                                                               session_uid=self.session_uid, store_fk=self.store_id,
                                                               display_text=price_class.replace("'", "''"),
                                                               visit_date=self.visit_date.isoformat(),
                                                               calculation_time=datetime.utcnow().isoformat(),
                                                               kps_name=PURINA_SET,
                                                                kpi_fk=kpi_fk )
                            else:
                                print 'atomic cannot be saved for price class {}'.format(price_class)


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
        #  This functions takes all brands, sub categories, categories and manufacturers in session.
        #  The function adds them to database in case they are new.
        brands = self.get_all_brands()
        sub_cats = self.get_all_sub_categories()
        manufacturer = self.get_all_manufacturers()
        cats = self.get_all_categories()
        prices = self.get_all_price_classes()

        new_brands = self.purina_scif.loc[~self.purina_scif['brand_name'].isin(brands)]['brand_name'].unique()
        new_manufacturer = self.purina_scif.loc[~self.purina_scif['manufacturer_name'].isin(manufacturer)][
            'manufacturer_name'].unique()
        new_sub_cat = self.purina_scif.loc[(~self.purina_scif[SCIF_SUB_CATEOGRY].isin(sub_cats)) &
                                        (~pd.isnull(self.purina_scif[SCIF_SUB_CATEOGRY]))][SCIF_SUB_CATEOGRY].unique()
        new_cat = self.purina_scif.loc[(~self.purina_scif[SCIF_CATEOGRY].isin(cats)) &
                                       (~pd.isnull(self.purina_scif[SCIF_CATEOGRY]))][SCIF_CATEOGRY].unique()
        new_prices = self.purina_scif.loc[(~self.purina_scif[SCIF_PRICE].isin(prices)) &
                                          (~pd.isnull(self.purina_scif[SCIF_PRICE]))][SCIF_PRICE].unique()

        self.save_static_atomics(BRAND, new_brands, PURINA_SET)
        self.save_static_atomics(MANUFACTUR, new_manufacturer, PURINA_SET)
        self.save_static_atomics(CATEGORY, new_cat, PURINA_SET)
        self.save_static_atomics(SUB_CATEGORY, new_sub_cat, PURINA_SET)
        self.save_static_atomics(PRICE_KPI, new_prices, PURINA_SET)

        self.commit_static_data()

    def get_all_brands(self):
        return self.kpi_static_data.loc[self.kpi_static_data['kpi_name'] == BRAND]['atomic_kpi_name']

    def get_all_sub_categories(self):
        return self.kpi_static_data.loc[self.kpi_static_data['kpi_name'] == SUB_CATEGORY]['atomic_kpi_name']

    def get_all_manufacturers(self):
        return self.kpi_static_data.loc[self.kpi_static_data['kpi_name'] == MANUFACTUR]['atomic_kpi_name']

    def get_all_categories(self):
        return self.kpi_static_data.loc[self.kpi_static_data['kpi_name'] == CATEGORY]['atomic_kpi_name']

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
        local_con = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        query = """select category_fk, resolution_fk, exclude_status_fk from probedata.session_category
                where session_fk = {}""".format(self.session_fk)
        # query = """select c.category_fk, s.resolution_code_fk, s.exclude_status_fk from probedata.session_category c
        #             join probedata.session s on s.pk=c.session_fk
        #           where session_fk = {}""".format(self.session_fk)
        data = pd.read_sql_query(query, local_con.db)
        return data