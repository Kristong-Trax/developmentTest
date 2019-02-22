
import os
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.CCBOTTLERSUS_SAND.DISPLAYS.GeneralToolBox import GENERALToolBox
from Projects.CCBOTTLERSUS_SAND.DISPLAYS.Fetcher import Queries
from Projects.CCBOTTLERSUS_SAND.DISPLAYS.ParseTemplates import parse_template

__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'Template.xlsx')


class Consts(object):

    KPI_NAME = 'KPI Name'
    SCENE_RECOGNITION = 'SR'
    MANUFACTURERS = 'Manufacturer'
    MANUFACTURERS_TO_EXCLUDE = 'Excluded Manufacturer'
    BRANDS = 'Additional Brands'
    BRANDS_TO_EXCLUDE = 'Brands Excluded'
    SSD_OR_STILL = 'SSD Still (att4)'
    ATT2 = 'att2'
    SOS_TARGET = 'Bay SOS Facings Target'
    SET_NAME = 'Manufacturer Displays'
    SEPARATOR = ','


class DISPLAYSToolBox(Consts):

    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output, common_db2):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        # self.all_products = self.all_products.merge(self.get_additional_attributes(), on='product_fk', how='left')
        self.match_display_in_scene = self.get_match_display()
        self.tools = GENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn, scif=self.scif)
        self.template_data = parse_template(TEMPLATE_PATH)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.common = common_db2

    # def get_additional_attributes(self):
    #     query = Queries.get_attributes_data()
    #     attributes = pd.read_sql_query(query, self.rds_conn.db)
    #     return attributes

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = Queries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        match_display = match_display.merge(self.scif.drop_duplicates(subset=['scene_fk']), on='scene_fk', how='left')
        return match_display

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = Queries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        kpi_results = {self.kpi_static_data.iloc[k]['kpi_name']: [0, 0, 0, 0, 0, 0] for k in xrange(len(
            self.kpi_static_data))
                       if self.kpi_static_data.iloc[k]['kpi_fk'] not in (195, 196, 197, 198, 199, 200, 201, 202)}
        relevant_displays = self.template_data[self.SCENE_RECOGNITION].unique().tolist()
        actual_displays = self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(
            relevant_displays)]
        for scene in actual_displays['scene_fk'].unique():
            scene_displays = actual_displays[actual_displays['scene_fk'] == scene]
            calculate_all = False
            if scene_displays.iloc[0]['template_name'] == 'Display (End Cap/Palette Drop/Rack)' \
                    and scene_displays['display_name'].unique().tolist() == ['Display']:
                display_type = scene_displays['display_name'].values[0]
                kpi_data = self.template_data[self.template_data[self.SCENE_RECOGNITION] == display_type]
                bays = sorted(scene_displays['bay_number'].unique().tolist())
                bay_groups = [[]]
                for bay in bays:
                    if len(bay_groups[-1]) < 3:
                        bay_groups[-1].append(bay)
                    else:
                        bay_groups.append([bay])
                for i, group in enumerate(bay_groups):
                    display = scene_displays.iloc[0]
                    display['bay_number'] = group
                    for p in xrange(len(kpi_data)):
                        params = kpi_data.iloc[p]
                        score = self.calculate_facing_sos(params, display)
                        for x, s in enumerate(score):
                            kpi_results[params[self.KPI_NAME]][x*3 + 1] += s
                        if len(bay_groups) == 1 or len(bay_groups[-1]) == 3 or i + 2 < len(bay_groups):
                            for x, s in enumerate(score):
                                kpi_results[params[self.KPI_NAME]][x*3 + 2] += s
                        elif i + 2 == len(bay_groups):
                            display['bay_number'] = group + bay_groups[i + 1]
                            score = self.calculate_facing_sos(params, display)
                            for x, s in enumerate(score):
                                kpi_results[params[self.KPI_NAME]][x*3 + 2] += s
            else:
                calculate_all = True
            for d in xrange(len(scene_displays)):
                display = scene_displays.iloc[d]
                display_name = display['display_name']
                kpi_data = self.template_data[self.template_data[self.SCENE_RECOGNITION] == display_name]
                for p in xrange(len(kpi_data)):
                    params = kpi_data.iloc[p]
                    score = self.calculate_facing_sos(params, display)
                    for x, s in enumerate(score):
                        if display_name == 'Display':
                            kpi_results[params[self.KPI_NAME]][x*3 + 0] += s
                        else:
                            kpi_results[params[self.KPI_NAME]][x] += s
                    if calculate_all and display['display_name'] == 'Display':
                        for x, s in enumerate(score):
                            kpi_results[params[self.KPI_NAME]][x*3 + 1] += s
                            kpi_results[params[self.KPI_NAME]][x*3 + 2] += s
        for kpi_name in kpi_results.keys():
            kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
            self.common.write_to_db_result(fk=kpi_fk, result=kpi_results[kpi_name][0], should_enter=True,
                                           identifier_parent=self.common.get_dictionary(kpi_name=self.SET_NAME),
                                           numerator_id=1, denominator_id=self.store_id)
            self.write_to_db_result(kpi_name, 100, level=self.LEVEL2)
            self.write_to_db_result(kpi_name, kpi_results[kpi_name], level=self.LEVEL3)
        self.write_to_db_result(self.SET_NAME, 100, level=self.LEVEL1)
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(self.SET_NAME)
        self.common.write_to_db_result(fk=kpi_fk, result=100,
                                       numerator_id=1, denominator_id=self.store_id,
                                       identifier_result=self.common.get_dictionary(kpi_name=self.SET_NAME))

    def calculate_facing_sos(self, params, display):
        filters = self.get_filters(params)
        numerator = self.tools.calculate_availability(bay_number=display['bay_number'],
                                                      product_type=('Empty', self.tools.EXCLUDE_FILTER),
                                                      scene_id=display['scene_fk'], **filters)
        denominator = self.tools.calculate_availability(bay_number=display['bay_number'],
                                                        product_type=('Empty', self.tools.EXCLUDE_FILTER),
                                                        scene_id=display['scene_fk'])
        result = 0 if denominator == 0 else numerator / float(denominator)
        target = map(float, str(params[self.SOS_TARGET]).split(self.SEPARATOR))
        scores = []
        for t in target:
            scores.append(1 if result > t else 0)
        return scores

    def get_filters(self, params):
        products = set()
        if params[self.MANUFACTURERS]:
            products = products.union(self.get_product_list(manufacturer_name=params[self.MANUFACTURERS].split(self.SEPARATOR)))
            if params[self.BRANDS]:
                products = products.union(self.get_product_list(att2=params[self.BRANDS].split(self.SEPARATOR)))
        else:
            products = products.union(self.all_products['product_ean_code'].unique().tolist())
        if params[self.SSD_OR_STILL]:
            products = products.intersection(self.get_product_list(att4=params[self.SSD_OR_STILL]))
        if params[self.MANUFACTURERS_TO_EXCLUDE]:
            products = products.difference(
                self.get_product_list(manufacturer_name=params[self.MANUFACTURERS_TO_EXCLUDE].split(self.SEPARATOR)))
        if params[self.BRANDS_TO_EXCLUDE]:
            products = products.difference(
                self.get_product_list(att2=params[self.BRANDS_TO_EXCLUDE].split(self.SEPARATOR)))
        products = products.difference(self.get_product_list(manufacturer_name='GENERAL'))
        # products = self.all_products[self.all_products['att2'].isin(params[self.ATT2].split(self.SEPARATOR))]['product_ean_code'].unique().tolist()
        return dict(product_ean_code=list(products))

    def get_product_list(self, **filters):
        product_list = self.all_products[self.tools.get_filter_condition(self.all_products, **filters)]
        return set(product_list['product_ean_code'].unique().tolist())

    def write_to_db_result(self, kpi_name, score, level):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(kpi_name, score, level)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL2:
            table = KPK_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, name, score, level):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == name]['kpi_set_fk'].values[0]
            attributes = pd.DataFrame([(name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), set_fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == name]['kpi_fk'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        kpi_fk, name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name',
                                               'score'])
        elif level == self.LEVEL3:
            score, score_2, score_3, result, result_2, result_3 = score
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == name]
            atomic_fk = data['atomic_kpi_fk'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = data['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, score_2, score_3, result, result_2, result_3, kpi_fk, atomic_fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'score_2', 'score_3', 'result',
                                               'result_2', 'result_3', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self, kpi_set_fk):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        kpi_pks = tuple()
        atomic_pks = tuple()
        if kpi_set_fk is not None:
            query = Queries.get_atomic_pk_to_delete(self.session_uid, kpi_set_fk)
            kpi_atomic_data = pd.read_sql_query(query, self.rds_conn.db)
            atomic_pks = tuple(kpi_atomic_data['pk'].tolist())
            kpi_data = pd.read_sql_query(query, self.rds_conn.db)
            kpi_pks = tuple(kpi_data['pk'].tolist())
        cur = self.rds_conn.db.cursor()
        if kpi_pks and atomic_pks:
            delete_queries = Queries.get_delete_session_results_query(self.session_uid, kpi_set_fk, kpi_pks,
                                                                      atomic_pks)
            for query in delete_queries:
                cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
