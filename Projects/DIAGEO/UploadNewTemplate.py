
import os
import pandas as pd
import json
from datetime import datetime, timedelta

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Aws.S3Connector import BucketConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEO.ToolBox import DIAGEOToolBox as Consts

__author__ = 'Nimrod'

BUCKET = 'traxuscalc'


class NewTemplate:

    PRODUCT_FIELDS_TO_VALIDATE = {'product_ean_code': [Consts.PRODUCT_EAN_CODE, Consts.PRODUCT_EAN_CODE2,
                                                       Consts.TESTED, Consts.ANCHOR],
                                  'brand_name': [Consts.BRAND_NAME],
                                  'sub_brand_name': [Consts.SUB_BRAND_NAME]}
    TEMPLATE_FIELDS_TO_VALIDATE = {'template_name': [Consts.LOCATION]}
    REQUIRED_FIELDS_FOR_SET = {'MPA': [(Consts.PRODUCT_EAN_CODE, Consts.PRODUCT_EAN_CODE2)],
                               'New Products': [(Consts.PRODUCT_EAN_CODE, Consts.PRODUCT_EAN_CODE2)],
                               'Relative Position': [Consts.CHANNEL, Consts.LOCATION, Consts.TESTED, Consts.ANCHOR,
                                                     Consts.TOP_DISTANCE, Consts.BOTTOM_DISTANCE, Consts.LEFT_DISTANCE,
                                                     Consts.RIGHT_DISTANCE],
                               'Brand Blocking': [Consts.BRAND_NAME],
                               'Brand Pouring': [Consts.BRAND_NAME],
                               'Survey Questions': [Consts.SURVEY_ANSWER, Consts.SURVEY_QUESTION]}
    SEPARATOR = ','

    def __init__(self, project):
        self.project = project
        self.log_suffix = '{}: '.format(self.project)
        self.templates_path = '{}{}/{}'.format(Consts.TEMPLATES_PATH, self.project, {})
        self.templates_to_upload = {}
        self.kpi_name_fields = (Consts.KPI_NAME, Consts.GROUP_NAME, Consts.PRODUCT_NAME)

    @property
    def amz_conn(self):
        if not hasattr(self, '_amz_conn'):
            self._amz_conn = BucketConnector(BUCKET)
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
    def templates_data(self):
        if not hasattr(self, '_template_data'):
            self._template_data = self.get_templates_data()
        return self._template_data

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
                select p.product_name, p.product_ean_code, sb.name as sub_brand_name,
                       b.name as brand_name, c.name as category, m.name as manufacturer
                from static.product p
                join static.sub_brand sb on p.sub_brand_fk = sb.pk
                join static.brand b on p.brand_fk = b.pk
                join static.product_categories c on b.category_fk = c.pk
                join static.manufacturers m on b.manufacturer_fk = m.pk
        """
        products_data = pd.read_sql_query(query, self.rds_conn.db)
        return products_data

    def get_templates_data(self):
        query = """
                select t.name as template_name, l.name as location_name
                from static.template t
                join static.location_types l on l.pk = t.location_type
        """
        templates_data = pd.read_sql_query(query, self.rds_conn.db)
        return templates_data

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
        data = Consts.get_json_data(template_path)
        if data is None:
            return False
        # saving data as a temp Json file
        template_path = '{}/{}_temp'.format(os.getcwd(), set_name)
        with open(template_path, 'wb') as f:
            json.dump(data, f)

        if set_name == 'Activation Standard':
            self.templates_to_upload[set_name] = template_path
            return True

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
                if field_name == Consts.GROUP_NAME:
                    kpis_to_add.append(params)
                elif value not in kpi_names:
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
        latest_date = Consts.get_latest_directory_date_from_cloud(self.templates_path.format(''), self.amz_conn)
        if latest_date:
            for file_path in [f.key for f in self.amz_conn.bucket.list(self.templates_path.format(latest_date))]:
                file_name = file_path.split('/')[-1]
                latest_templates[file_name] = file_path
        return latest_templates

    def add_dummy_kpi(self, set_name):
        """
        This function receives a KPI set name (which already exists in static.kpi_set) and adds one KPI and one
        Atomic KPI to the DB. (This is done for sets which do not have a template.)
        """
        params = {Consts.PRODUCT_NAME: set_name,
                  Consts.KPI_NAME: set_name}
        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
        self.add_new_kpi_to_static_tables(set_fk, new_kpi_list=[params])

    def add_activation_standard_kpis(self, template_path):
        set_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == 'Activation Standard']
        self.delete_kpis(set_data['kpi_fk'].unique().tolist())
        set_fk = set_data['kpi_set_fk'].values[0]
        data = Consts.get_json_data(template_path)
        new_kpis = []
        for pillar in data:
            name = pillar.get(Consts.ACTIVATION_KPI_NAME)
            if not name:
                name = pillar.get(Consts.ACTIVATION_SET_NAME)
            weight = pillar.get(Consts.ACTIVATION_WEIGHT)
            new_kpis.append({Consts.KPI_NAME: name, 'Weight': weight})
        self.add_new_kpi_to_static_tables(set_fk, new_kpis)

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
                        product_ean_code = str(kpi.get(Consts.PRODUCT_EAN_CODE, kpi.get(Consts.PRODUCT_EAN_CODE2, '')))
                        product_ean_code = '' if len(product_ean_code.split()) > 1 else product_ean_code
                        level2_query = """
                                       INSERT INTO static.kpi (kpi_set_fk, display_text, display_attribute)
                                       VALUES ('{0}', '{1}', '{2}');""".format(set_fk, kpi_name_safe, product_ean_code)
                        cur.execute(level2_query)
                        level2_added.append(kpi_name)
                        kpi_fk = cur.lastrowid
                        if kpi.get('Weight'):
                            weight_query = "UPDATE static.kpi SET weight = '{}' WHERE pk = {}".format(kpi.get('Weight'),
                                                                                                      kpi_fk)
                            cur.execute(weight_query)
                    else:
                        kpi_fk = existing.iloc[0]['kpi_fk']
                if name_field == Consts.GROUP_NAME:
                    products = str(kpi.get(Consts.PRODUCT_EAN_CODE, kpi.get(Consts.PRODUCT_EAN_CODE2, ''))).strip().split()
                    names = kpi.get(Consts.PRODUCT_NAME).strip().split("\n")
                    for index, product in enumerate(products):
                        existing = self.kpi_static_data[(self.kpi_static_data['kpi_fk'] == kpi_fk) &
                        #                                 (self.kpi_static_data['description'] == product)]
                                                        (self.kpi_static_data['atomic_kpi_name'] == names[index])]
                        if existing.empty:
                            name = names[index]
                            name_safe = name.replace("'", "\\'").encode('utf-8')
                            level3_query = """
                                INSERT INTO static.atomic_kpi (kpi_fk, name, description, display_text,
                                                               presentation_order, display)
                                VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(kpi_fk,
                                                                                             name_safe,
                                                                                             product,
                                                                                             name_safe,
                                                                                             1, 'Y')
                            cur.execute(level3_query)
                            level3_added.append(name)
                else:
                    existing = self.kpi_static_data[(self.kpi_static_data['kpi_fk'] == kpi_fk) &
                                                    (self.kpi_static_data['atomic_kpi_name'] == kpi_name)]
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
                        level3_added.append(kpi_name)
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
        if not set(fields_of_data).intersection(self.kpi_name_fields):
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
        non_existent_products = set()
        non_existent_templates = set()
        for params in data:
            for entity in self.PRODUCT_FIELDS_TO_VALIDATE.keys():
                for field in self.PRODUCT_FIELDS_TO_VALIDATE[entity]:
                    values = str(params.get(field, '')).split(self.SEPARATOR)
                    if ''.join(values):
                        for value in values:
                            if self.products_data[self.products_data[entity] == value].empty:
                                non_existent_products.add(value)
            for entity in self.TEMPLATE_FIELDS_TO_VALIDATE.keys():
                for field in self.TEMPLATE_FIELDS_TO_VALIDATE[entity]:
                    values = params.get(field, '')
                    values = values if isinstance(values, (str, unicode)) else str(values)
                    values = values.split(self.SEPARATOR)
                    if ''.join(values):
                        for value in values:
                            if self.templates_data[self.templates_data[entity] == value].empty:
                                non_existent_templates.add(value)
        if non_existent_products:
            Log.warning(self.log_suffix + 'The following products/brands/categories do not exist: {}'.format(', '.join(non_existent_products)))
        if non_existent_templates:
            Log.warning(self.log_suffix + 'The following templates/locations do not exist: {}'.format(', '.join(non_existent_templates)))
        if not ignore_missings and (non_existent_products or non_existent_templates):
            return False
        return True

    def add_kpi_sets_to_static(self, new_sets=Consts.KPI_SETS):
        """
        This function is to be ran at a beginning of a projects - and adds the constant KPI sets data to the DB.
        """
        cur = self.rds_conn.db.cursor()
        for set_name in new_sets:
            if self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name].empty:
                level1_query = """
                               INSERT INTO static.kpi_set (name, missing_kpi_score, enable, normalize_weight,
                                                           expose_to_api, is_in_weekly_report)
                               VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(set_name, 'Bad', 'Y',
                                                                                            'N', 'N', 'N')
                cur.execute(level1_query)
        self.rds_conn.db.commit()


# if __name__ == '__main__':
#     Config.init()
#     LoggerInitializer.init('New Template')
#     for project_name in ['diageouk-sand']:
#         template = NewTemplate(project_name)
#         # for name in ['Visible to Customer']:
#         #     template.add_dummy_kpi(name)
#         # template.add_kpi_sets_to_static(['Local MPA'])
#         # path = '/home/Nimrod/Documents/DiageoTWTemplates/Activation Standard.xlsx'
#         # template.add_activation_standard_kpis(path)
#         for set_name in ['MPA','Local MPA']:
#             file_path = '/home/Elyashiv/Shivi/KPIs/templates/diageoUK/11.01.18/{}.xlsx'.format(set_name)
#             r = template.handle_updated_template(set_name, file_path, ignore_missings=True)
#             if not r:
#                 quit()
#         template.upload_new_templates(immediate_change=True)
