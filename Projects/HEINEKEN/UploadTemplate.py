
import os
import pandas as pd
import json
from datetime import datetime, timedelta

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Cloud.Services.Storage.Factory import StorageFactory
from Trax.Utils.Conf.Configuration import Config
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


__author__ = 'Nimrod'

BUCKET = 'traxuscalc'

class Consts(object):
    TEMPLATES_PATH = 'Heineken_templates/'
    KPI_NAME = u'KPI Name'
    KPI_SETS = ['Availability']
    PRODUCT_NAME = u'Product English Name'
    PRODUCT_EAN_CODE = u'EAN Code'
    ENTITY_TYPE = u'Entity'
    BRAND_FK = u'Brand pk'
    BRAND_NAME = u'Brand English Name'
    CATEGORY_FK = u'Category pk'
    CATEGORY_NAME = u'Category English Name'
    SURVEY_Q_FK = u'Survey Question ID'
    SURVEY_Q_TEXT = u'Survey Text'
    SURVEY_A_TEXT = u'Survey Answer'
    STORE_TYPE = u'Store Type'


class NewTemplate:

    PRODUCT_FIELDS_TO_VALIDATE = {'product_ean_code': [Consts.PRODUCT_EAN_CODE],
                                  'brand_name': [Consts.BRAND_NAME],
                                  'brand_fk': [Consts.BRAND_FK],
                                  'category_name': [Consts.CATEGORY_NAME],
                                  'category_fk': [Consts.CATEGORY_FK]}

    TEMPLATE_FIELDS_TO_VALIDATE = {'store_type': [Consts.STORE_TYPE]}

    REQUIRED_FIELDS_FOR_ENTITY = {'Product': [Consts.PRODUCT_EAN_CODE],
                                  'Survey': [Consts.SURVEY_Q_FK, Consts.SURVEY_A_TEXT],
                                  'Brand': [Consts.BRAND_FK],
                                  'Brand,Category': [Consts.BRAND_FK, Consts.CATEGORY_FK],
                                  'Category': [Consts.CATEGORY_FK]}
    REQUIRED_FIELDS_FOR_SET = {'Availability': [Consts.KPI_NAME, Consts.ENTITY_TYPE]}
    SEPARATOR = ','

    def __init__(self, project):
        self.project = project
        self.log_suffix = '{}: '.format(self.project)
        self.templates_path = '{}{}/{}'.format(Consts.TEMPLATES_PATH, self.project, {})
        self.templates_to_upload = {}
        self.kpi_name_fields = [Consts.KPI_NAME]

    @property
    def amz_conn(self):
        if not hasattr(self, '_amz_conn'):
            self._amz_conn = StorageFactory.get_connector(BUCKET)
        return self._amz_conn

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = AwsProjectConnector(self.project, DbUsers.CalculationEng)
        return self._rds_conn

    @property
    def kpi_static_data(self):
        if not hasattr(self, '_kpi_static_data'):
            self._kpi_static_data = self.get_kpi_data()
        return self._kpi_static_data

    @property
    def products_data(self):
        if not hasattr(self, '_product_data'):
            self._products_data = self.get_products_data()
        return self._products_data

    @property
    def stores_data(self):
        if not hasattr(self, '_store_data'):
            self._stores_data = self.get_stores_data()
        return self._stores_data

    def get_kpi_data(self):
        query = """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk, api.description,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.kpi_set kps
            left join static.kpi kpi on kps.pk = kpi.kpi_set_fk
            left join static.atomic_kpi api on kpi.pk = api.kpi_fk
        """
        kpi_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_data

    def get_products_data(self):
        query = """
                SELECT
                    p.name AS product_name,
                    p.ean_code AS product_ean_code,
                    p.category_fk as category_fk,
                    p.brand_fk as brand_fk,
                    b.name AS brand_name,
                    m.name AS manufacturer,
                    c.name AS category_name
                FROM
                    static_new.product p
                        JOIN
                    static_new.brand b ON p.brand_fk = b.pk
                        JOIN
                    static_new.manufacturer m ON b.manufacturer_fk = m.pk
                        JOIN
                    static_new.category c ON c.pk = p.category_fk
                """
        products_data = pd.read_sql_query(query, self.rds_conn.db)
        return products_data

    def get_stores_data(self):
        query = """
                select s.name as store_name, s.store_type as store_type
                from static.stores s
        """
        stores_data = pd.read_sql_query(query, self.rds_conn.db)
        return stores_data

    def delete_kpis(self, kpi_list):
        if kpi_list:
            queries = ["delete from static.kpi where pk in {}".format(tuple(kpi_list)),
                       "delete from static.atomic_kpi where kpi_fk in {}".format(tuple(kpi_list))]
            cur = self.rds_conn.db.cursor()
            for query in queries:
                cur.execute(query)
            self.rds_conn.db.commit()

    def handle_updated_template(self, set_name, template_path, ignore_missings=False, replace_existing=False):
        """
        :param set_name: The relevant KPI set name.
        :param template_path: The full path of the template file.
        :param ignore_missings: True - for always validating the template (disregarding missing products);
                                False - for validating only in case all products exist in the DB.
        :param replace_existing: True - replacing existing KPIs (of this set) with the current one;
                                 False - adding new KPIs as addition to the existing ones.
        :return: True if the new templates has been successfully processed and updated;
                 False if the template contains false data.
        """
        data = self.get_json_data(template_path)
        if data is None:
            return False
        # saving data as a temp Json file
        template_path = '{}/{}_temp'.format(os.getcwd(), set_name)
        with open(template_path, 'wb') as f:
            json.dump(data, f)

        # validate template
        is_validated = self.validate_template(set_name, data, ignore_missings=ignore_missings)
        if not is_validated:
            os.remove(template_path)
            return False
        Log.info(self.log_suffix + 'Template validated successfully for set {}'.format(set_name))
        # write new KPIs to static tables
        set_static_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]
        if set_static_data.empty:
            Log.info(self.log_suffix + 'Set name does not exist')
            os.remove(template_path)
            return False
        set_fk = set_static_data['kpi_set_fk'].values[0]
        if not replace_existing:
            kpi_names = set_static_data['kpi_name'].unique().tolist()
            kpis_to_add = []
            for params in data:
                field_name, value = self.get_kpi_name(params)
                if field_name == Consts.KPI_NAME and value not in kpi_names:
                    kpis_to_add.append(params)

            level2_counter, level3_counter = self.add_new_kpi_to_static_tables(set_fk, kpis_to_add)
            Log.info(self.log_suffix + '{} KPIs and {} Atomics were added'.format(level2_counter, level3_counter))
        else:
            kpi_list = set_static_data['kpi_fk'].unique().tolist()
            kpi_list = filter(lambda x: isinstance(x, int), kpi_list)
            self.delete_kpis(kpi_list)
            level2_counter, level3_counter = self.add_new_kpi_to_static_tables(set_fk, data)
            Log.info(self.log_suffix + '{} KPIs were deleted; {} KPIs and {} Atomics were added'.format(len(kpi_list),
                                                                                                        level2_counter,
                                                                                                        level3_counter))
        self.templates_to_upload[set_name] = template_path
        Log.info(self.log_suffix + 'Template {} is ready to be uploaded'.format(set_name))
        return True

    def get_kpi_name(self, params):
        """
        This function extracts the KPI name (for DB purposes), by a certain hierarchy.
        """
        for name_field in self.kpi_name_fields:
            if params.get(name_field):
                return name_field, params.get(name_field)
        return None, None

    def upload_new_templates(self, immediate_change=False):
        """
        This function uploads the new template, along with the latest version of the rest of the templates,
        to a new directory (with name as the current date's) in the Cloud.
        """
        if not self.templates_to_upload:
            Log.info(self.log_suffix + 'No new templates are ready for upload')
        else:
            if not immediate_change:
                next_day = (datetime.utcnow() + timedelta(1)).strftime("%y%m%d")
            else:
                next_day = datetime.utcnow().strftime("%y%m%d")
            templates_path_in_cloud = self.templates_path.format(next_day)
            latest_templates = self.get_latest_templates()
            for set_name in self.templates_to_upload:
                self.amz_conn.save_file(templates_path_in_cloud, set_name, self.templates_to_upload[set_name])
                os.remove(self.templates_to_upload[set_name])
                if set_name in latest_templates:
                    latest_templates.pop(set_name)
            Log.info(self.log_suffix + 'New templates for sets {} were uploaded'.format(self.templates_to_upload.keys()))

            for template_name in latest_templates:
                temp_file_path = '{}/{}_temp'.format(os.getcwd(), template_name)
                f = open(temp_file_path, 'wb')
                self.amz_conn.download_file(latest_templates[template_name], f)
                f.close()
                self.amz_conn.save_file(templates_path_in_cloud, template_name, temp_file_path)
                os.remove(temp_file_path)
            Log.info(self.log_suffix + 'Existing templates for sets {} were aligned with the new ones'.format(latest_templates.keys()))
            return True

    def get_latest_templates(self):
        """
        This function returns the paths of the latest documented templates in the Cloud.
        """
        latest_templates = {}
        latest_date = self.get_latest_directory_date_from_cloud(self.templates_path.format(''), self.amz_conn)
        if latest_date:
            for file_path in [f.key for f in self.amz_conn.bucket.list(self.templates_path.format(latest_date))]:
                file_name = file_path.split('/')[-1]
                latest_templates[file_name] = file_path
        return latest_templates


    def add_new_kpi_to_static_tables(self, set_fk, new_kpi_list):
        """
        :param set_fk: The relevant KPI set FK.
        :param new_kpi_list: a list of all new KPI's parameters.
        This function adds new KPIs to the DB ('Static' table) - both to level2 (KPI) and level3 (Atomic KPI).
        """
        level2_added = []
        level3_added = []
        cur = self.rds_conn.db.cursor()
        for kpi in new_kpi_list:
            if kpi:
                name_field, kpi_name = self.get_kpi_name(kpi)
                kpi_name_safe = kpi_name.replace("'", "\\'").encode('utf-8')
                if kpi_name in level2_added:
                    continue
                else:
                    existing = self.kpi_static_data[(self.kpi_static_data['kpi_set_fk'] == set_fk) &
                                                    (self.kpi_static_data['kpi_name'] == kpi_name)]
                    if existing.empty:
                        level2_query = """
                                       INSERT INTO static.kpi (kpi_set_fk, display_text)
                                       VALUES ('{0}', '{1}');""".format(set_fk, kpi_name_safe)
                        cur.execute(level2_query)
                        level2_added.append(kpi_name)
                        kpi_fk = cur.lastrowid

                    else:
                        kpi_fk = existing.iloc[0]['kpi_fk']

                if kpi_name in level3_added:
                    continue
                else:
                    existing = self.kpi_static_data[(self.kpi_static_data['kpi_set_fk'] == set_fk) &
                                                    (self.kpi_static_data['kpi_fk'] == kpi_fk) &
                                                    (self.kpi_static_data['description'] == kpi)]
                    if existing.empty:
                        level3_query = """
                            INSERT INTO static.atomic_kpi (kpi_fk, name, description, display_text,
                                                           presentation_order, display)
                            VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(kpi_fk,
                                                                                         kpi_name_safe,
                                                                                         kpi_name_safe,
                                                                                         kpi_name_safe,
                                                                                         1, 'Y')
                        cur.execute(level3_query)
                        level3_added.append(kpi)

                    else:
                        Log.info('Atomic kpi \'{}\' already exist'.format(kpi_name))

        self.rds_conn.db.commit()
        return len(level2_added), len(level3_added)

    def validate_template(self, set_name, data, ignore_missings=False):
        """
        This function goes through the template content and checks whether the data is correct.
        If so, it returns True. Else, it returns False.
        """
        if not data:
            Log.error(self.log_suffix + 'Template for set {} has no data'.format(set_name))
            return False
        fields_of_data = data[0].keys()
        kpi_name_fields_set = set([self.kpi_name_fields]) if not isinstance(self.kpi_name_fields, (set,tuple,list)) \
            else self.kpi_name_fields

        if not set(fields_of_data).intersection(kpi_name_fields_set):
            Log.error(self.log_suffix + "The template for '{}' must have a '{}' column, to set the KPI name".format(set_name, ' / '.join(self.kpi_name_fields)))
            return False
        if set_name in self.REQUIRED_FIELDS_FOR_SET.keys():
            required_fields = self.REQUIRED_FIELDS_FOR_SET[set_name]
            missing_fields = []
            for field in required_fields:
                if not isinstance(field, (list, tuple)):
                    if field not in fields_of_data:
                        missing_fields.append(field)
                else:
                    appeared = False
                    for f in field:
                        if f in fields_of_data:
                            appeared = True
                            break
                    if not appeared:
                        missing_fields.append(' / '.join(field))
            if missing_fields:
                Log.error(self.log_suffix + '{} template must contain the following columns: {}'.format(set_name.capitalize(), ', '.join(missing_fields)))
                return False
            if Consts.ENTITY_TYPE in required_fields:
                if not self.validate_template_entity(data):
                    return False
        non_existent_products = set()
        non_existent_stores = set()
        for params in data:
            for entity in self.PRODUCT_FIELDS_TO_VALIDATE.keys():
                for field in self.PRODUCT_FIELDS_TO_VALIDATE[entity]:
                    values = str(params.get(field, '')).split(self.SEPARATOR)
                    if ''.join(values):
                        for value in values:
                            try:
                                if self.products_data[self.products_data[entity] == value].empty:
                                    non_existent_products.add(value)
                            except TypeError:
                                if self.products_data[self.products_data[entity] == float(value)].empty:
                                    non_existent_products.add(value)

            for entity in self.TEMPLATE_FIELDS_TO_VALIDATE.keys():
                for field in self.TEMPLATE_FIELDS_TO_VALIDATE[entity]:
                    values = params.get(field, '')
                    values = values if isinstance(values, (str, unicode)) else str(values)
                    values = values.split(self.SEPARATOR)
                    if ''.join(values):
                        for value in values:
                            if self.stores_data[self.stores_data[entity] == value].empty:
                                non_existent_stores.add(value)
        if non_existent_products:
            Log.warning(self.log_suffix + 'The following products/brands/categories do not exist: {}'.format(', '.join(non_existent_products)))
        if non_existent_stores:
            Log.warning(self.log_suffix + 'The following store types do not exist: {}'.format(', '.join(non_existent_templates)))
        if not ignore_missings and (non_existent_products or non_existent_stores):
            return False
        return True

    def validate_template_entity(self, data):
        """
        This checks if all entities given in template are valid.
        Returns True if so, False otherwise

        """

        template = pd.DataFrame(data)
        invalid_entity = template.loc[~template[Consts.ENTITY_TYPE].isin(self.REQUIRED_FIELDS_FOR_ENTITY.keys())]
        if not invalid_entity.empty:
            entities = ','.join(invalid_entity[Consts.ENTITY_TYPE].unique().tolist())
            Log.warning('Invalid Entities were found: {}'.format(entities))
            return False
        for entity in self.REQUIRED_FIELDS_FOR_ENTITY.keys():
            entity_filtered_template = template.loc[template[Consts.ENTITY_TYPE] == entity]
            if not entity_filtered_template.empty:
                if entity_filtered_template[self.REQUIRED_FIELDS_FOR_ENTITY[entity]].isnull().any().values[0]:
                    Log.warning('Required fields for entity {} are missing.'.format(entity))
                    return False
        return True

    def add_kpi_sets_to_static(self):
        """
        This function is to be ran at a beginning of a projects - and adds the constant KPI sets data to the DB.
        """
        cur = self.rds_conn.db.cursor()
        for set_name in Consts.KPI_SETS:
            if self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name].empty:
                level1_query = """
                               INSERT INTO static.kpi_set (name, missing_kpi_score, enable, normalize_weight,
                                                           expose_to_api, is_in_weekly_report)
                               VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(set_name, 'Bad', 'Y',
                                                                                            'N', 'N', 'N')
                cur.execute(level1_query)
        self.rds_conn.db.commit()

    @staticmethod
    def get_json_data(file_path):
        """
        This function gets a file's path and extract its content into a JSON.
        """
        output = pd.read_excel(file_path)
        if Consts.KPI_NAME not in output.keys() or Consts.ENTITY_TYPE not in output.keys():
            Log.warning('The required columns are missing')
            return None

        output = output[[key for key in output.keys() if isinstance(key, (str, unicode, int))]]
        duplicated_rows = set([head for head in output.keys() if output.keys().tolist().count(head) > 1])
        if duplicated_rows:
            Log.warning('The following columns titles appear more than once: {}'.format(', '.join(duplicated_rows)))
            return None
        output = output.to_json(orient='records')
        json_data = json.loads(output)
        # Removing None values + Converting all values to unicode-typed + Removing spaces from headers
        for i in xrange(len(json_data)):
            for key in json_data[i].keys():
                if json_data[i][key] is None:
                    json_data[i].pop(key)
                else:
                    json_data[i][key] = unicode(json_data[i][key]).strip()
                try:
                    json_data[i][unicode(key).strip()] = json_data[i].pop(key)
                except KeyError:
                    continue
        return filter(bool, json_data)

    @staticmethod
    def get_latest_directory_date_from_cloud(cloud_path, amz_conn):
        """
        This function reads all files from a given path (in the Cloud), and extracts the dates of their mother dirs
        by their name. Later it returns the latest date (up to today).
        """
        files = amz_conn.bucket.list(cloud_path)
        files = [f.key.replace(cloud_path, '') for f in files]
        files = [f for f in files if len(f.split('/')) > 1]
        files = [f.split('/')[0] for f in files]
        files = [f for f in files if f.isdigit()]
        if not files:
            return
        dates = [datetime.strptime(f, '%y%m%d') for f in files]
        for date in sorted(dates, reverse=True):
            if date.date() <= datetime.utcnow().date():
                return date.strftime("%y%m%d")
        return

# if __name__ == '__main__':
#     Config.init()
#     LoggerInitializer.init('New Template')
#     for project_name in ['heinekencn-sand']:
#         template = NewTemplate(project_name)
#         for set_name in ['Availability']:
#             file_path = '/home/Yasmin/Desktop/HeinekenCN/{}.xlsx'.format(set_name)
#             r = template.handle_updated_template(set_name, file_path, ignore_missings=True)
#             if not r:
#                 quit()
#         template.upload_new_templates(immediate_change=True)
