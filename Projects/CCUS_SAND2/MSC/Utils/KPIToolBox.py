import os
from datetime import datetime

import pandas as pd
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log

from Projects.CCUS_SAND2.MSC.Utils.Fetcher import MSCQueries
from Projects.CCUS_SAND2.MSC.Utils.GeneralToolBox import MSCGENERALToolBox
from Projects.CCUS_SAND2.MSC.Utils.ParseTemplates import parse_template

__author__ = 'Shani'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')

def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result
        return wrapper
    return decorator


class MSCToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.output = output
        self.data_provider = data_provider
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
        self.tools = MSCGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.custom_templates = {}
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.get_atts()
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.scif_filters = {'In Scene Type': 'template_name', 'Template Group': 'template_group', 'Excluded Template Group':'template_group', 'Manufacturer':'manufacturer_name',
                             'Manufacturer Exclude': 'manufacturer_name', 'Trademark (att2)': 'att2', 'Exluded Trademark (att2)':'att2', 'Sub Category': 'sub_category',
                             'Excluded Package Type (att3)': 'att3', 'Package Group (att1)': 'att1', 'SSD Still (att4)':'att4', 'Product Categories':'category', 'Package Type (att3)': 'att3',
                             'Excluded Product Categories':'category', 'Excluded EAN Code': 'product_ean_code', 'EAN Code': 'product_ean_code', 'Question fk':'Question fk'}
        self.row_filters =  ['In Scene Type', 'Template Group', 'Excluded Template Group', 'Manufacturer', 'Manufacturer Exclude', 'Trademark (att2)', 'Exluded Trademark (att2)', 'Sub Category',
                              'Package Type (att3)', 'Excluded Package Type (att3)', 'Package Group (att1)', 'SSD Still (att4)', 'Product Categories', 'Excluded Product Categories', 'Excluded EAN Code', 'EAN Code']

    def get_custom_template(self, name):
        if name not in self.custom_templates.keys():
            template = parse_template(TEMPLATE_PATH, sheet_name=name)
            if template.empty:
                template = parse_template(TEMPLATE_PATH, name, 2)
            self.custom_templates[name] = template
        return self.custom_templates[name]

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = MSCQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        score = 0

        # todo: to add condition to check whether the session belong to some of the store types that we calculate
        template = self.get_custom_template(self.store_type)
        kpis = template['Original KPI'].unique().tolist()
        for kpi in kpis:
            kpi_template = template.loc[template['Original KPI'] == kpi]
            groups_scores = {}
           # groups_list = kpi_template['Group'].unique().tolist()
#            groups_list = filter(None, groups_list)
            for index, row in kpi_template.iterrows():
                kpi_name = row['Name']
                if row['KPI Type'] == 'Availability':
                    score = self.calculate_availability(kpi, row)
                    # sum_of_kpis += score
                if row['KPI Type'] == 'Count Unique SKUs':
                    score = self.count_unique(kpi, row, assortment_entity='product_ean_code')
                if row['KPI Type'] == 'Count unique Trademarks':
                    score =self.count_unique(kpi, row, assortment_entity='att2')
                if row['KPI Type'] == 'Count of Facings':
                    score= self.count_facings(kpi, row)
                if row['KPI Type'] == 'SOS By KPIs':
                    score = self.count_sos_by_kpis(groups_scores, kpi, row)
                if row['KPI Type'] == 'Count unique Brands':
                    score= self.count_unique(kpi, row, assortment_entity='brand_fk')
                if row['KPI Type'] == 'survey question':
                    score = self.check_survey_answer(kpi, row)
                if row['KPI Type'] == 'SUM of KPI Group':
                    score = self.sum_of_kpi_groups(groups_scores, kpi, row)
                # save_to_db(sum_pf_kpis)
                # if not pd.isnull(row['Group']):
                if row['Group'] not in groups_scores:
                    groups_scores[row['Group']] = []
                groups_scores[row['Group']].append(score)
                #if row['level2'] == 'Y':
                    #kpi_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == kpi) &
                                                     #(self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
                    #self.write_to_db_result(kpi_fk, score=score, level=self.LEVEL2)


    def calculate_availability(self, kpi_name, row):
        atomic_name = row['Name']
        filters = self.get_filters_from_row(row)
        result = self.tools.calculate_availability(**filters)
        score = 1 if result >= 1 else 0
        #if row['Saved'] == 'Y':
            #atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
            #                                     #(self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
            #self.write_to_db_result(atomic_fk, score=score, level=self.LEVEL3, result=result)
        return score

    def count_facings(self, kpi_name, row):
        atomic_name = row['Name']
        filters = self.get_filters_from_row(row)
        result = self.tools.calculate_availability(**filters)
        score = 1 if result >= 1 else 0  # target is 1 facing
        #if row['Saved'] == 'Y':
            #atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                         #(self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
            #self.write_to_db_result(atomic_fk, score=score, level=self.LEVEL3, result=result)
        return score

    def count_unique(self, kpi_name, row, assortment_entity):
        atomic_name = row['Name']
        filters = self.get_filters_from_row(row)
        result = self.tools.calculate_assortment(assortment_entity=assortment_entity, **filters)
        if assortment_entity == 'brand_fk':
            score = 1 if result >= 2 else 0
        elif assortment_entity == 'att2':
            score = result
        else:
            score = result
        #if row['Saved'] == 'Y':
            #atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                         #(self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
            #self.write_to_db_result(atomic_name, result, level=self.LEVEL3)
        return score  # todo: is it ok?


    def check_survey_answer(self, kpi_name, row):
        """
        This function calculates 'Survey Question' typed KPI, and saves the results to the DB.
        """
        atomic_name = row['Name']
        target_answer = row['Target Answer']
        Questions = int(row['Question fk'].split(","))
        for question in Questions:
            survey_answer = self.tools.get_survey_answer(('question_fk', question))
            survey_answer = '-' if survey_answer is None else survey_answer
            if survey_answer == target_answer:
                score = 1
                result = 'Yes'
                #if row['Saved'] == 'Y':
                    #atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                                 #(self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
                     # self.write_to_db_result(atomic_fk, score=score, level=self.LEVEL3, result=result)
                return score
        score = 0
        result = 'No'
        #if row['Saved'] == 'Y':
            #atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                         #(self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
            # self.write_to_db_result(atomic_fk, score=score, level=self.LEVEL3, result=result)
        return score


    def sum_of_kpi_groups(self, groups_scores, kpi_name, row):
        atomic_name = row['Name']
        result = 0
        score = 1
        for group in row['Tested KPI Names'].split(","):
            result += sum(groups_scores.get(group))
        if row['Target Min'] is not u"" or None:
            if result < int(float(row['Target Min'])):
                score=0
        if row['Target MAX'] is not u"" or None:
            if result > int(float(row['Target MAX'])):
                score = 0
        #if row['Saved'] == 'Y':
            #atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                         #(self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
            #self.write_to_db_result(atomic_fk, score=score, level=self.LEVEL3, result=result)
        return score

    def count_sos_by_kpis(self, groups_scores, kpi_name, row):
        atomic_name = row['Name']
        sos_numerator = 0
        sos_denominator = 0
        for group in row['SOS Num'].split(","):
            sos_numerator += sum(groups_scores.get(group))
        for group in row['SOS Deno'].split(","):
            sos_denominator += sum(groups_scores.get(group))
        #sos_numerator=sum(groups_scores[row['SOS Num']])
        #sos_denominator=sum(groups_scores[row['SOS Deno']])
        if sos_denominator == 0:
            result = 0
            score = 0
        else:
            result = float(sos_numerator/sos_denominator)*100
            score = result
        #if row['Saved'] == 'Y':
            #atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                        # (self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
            # self.write_to_db_result(atomic_fk, score=score, level=self.LEVEL3, result=result)
        return score

    def get_atts(self):

        query = MSCQueries.get_product_atts()
        product_att3 = pd.read_sql_query(query, self.rds_conn.db)
        self.scif = self.scif.merge(product_att3, how='left', left_on='product_ean_code',
                                    right_on='product_ean_code')

    def get_filters_from_row(self, row):
        """
        :param row: the row in template to get filters from
        :param row_filters: the filters needed (columns names in template)
        :return: a dictionary of filters in scif
        """
        filters = {}
        for current_filter in self.row_filters:
            if row[current_filter] is not u"" or None:
                if current_filter in self.scif_filters:
                    if 'Exclude' in current_filter:
                        filters[self.scif_filters[current_filter]] = (row[current_filter].split(","), 'False')
                    else:
                        #general_filters['store_type'] = [str(g) for g in param.get('Store_Type').split(",")]
                        filters[self.scif_filters[current_filter]] = row[current_filter].split(",")
                else:
                    filters[current_filter] = row[current_filter]
        return filters

    def write_to_db_result(self, fk, score, level, result=None):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, score, level, result)
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

    def create_attributes_dict(self, fk, score, level, result=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk, result)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk', 'result'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self,kpi_set_fk=None):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        kpi_pks = tuple()
        atomic_pks = tuple()
        if kpi_set_fk is not None:
            query = MSCQueries.get_atomic_pk_to_delete(self.session_uid, kpi_set_fk)
            kpi_atomic_data = pd.read_sql_query(query, self.rds_conn.db)
            atomic_pks = tuple(kpi_atomic_data['pk'].tolist())
            kpi_data = pd.read_sql_query(query, self.rds_conn.db)
            kpi_pks = tuple(kpi_data['pk'].tolist())
        cur = self.rds_conn.db.cursor()
        if kpi_pks and atomic_pks:
            delete_queries = MSCQueries.get_delete_session_results_query(self.session_uid, kpi_set_fk, kpi_pks,
                                                                          atomic_pks)
            for query in delete_queries:
                cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
