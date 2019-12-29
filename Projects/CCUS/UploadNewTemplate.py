
import os
import pandas as pd
import json
from datetime import datetime, timedelta

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Cloud.Services.Storage.Factory import StorageFactory
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Projects.CCUS.Utils.ToolBox import ToolBox as Consts

__author__ = 'Nimrod'
AVAILABILITY_SETS = 'Dunkin Donuts'
BUCKET = 'traxuscalc'


class NewTemplate:

    PRODUCT_FIELDS_TO_VALIDATE = {'product_ean_code': [Consts.PRODUCT_EAN_CODE, Consts.PRODUCT_EAN_CODE2,
                                                       Consts.TESTED, Consts.ANCHOR],
                                  'brand_name': [Consts.BRAND_NAME],
                                  'sub_brand_name': [Consts.SUB_BRAND_NAME]}
    TEMPLATE_FIELDS_TO_VALIDATE = {'template_name': [Consts.LOCATION]}
    REQUIRED_FIELDS_FOR_SET = {'Availability': [(Consts.PRODUCT_EAN_CODE, Consts.PRODUCT_EAN_CODE2)],
                               'Relative Position': [Consts.CHANNEL, Consts.LOCATION, Consts.TESTED, Consts.ANCHOR,
                                                     Consts.TOP_DISTANCE, Consts.BOTTOM_DISTANCE, Consts.LEFT_DISTANCE,
                                                     Consts.RIGHT_DISTANCE],
                               'Brand Blocking': [Consts.BRAND_NAME],
                               'Brand Pouring': [Consts.BRAND_NAME]}
    SEPARATOR = ','
    TEMPLATES_PATH = 'DunkinDonuts_templates/'

    def __init__(self, project):
        self.project = project
        self.log_suffix = '{}: '.format(self.project)
        self.templates_path = '{}{}/{}'.format(self.TEMPLATES_PATH, self.project, {})
        self.kpi_name_field = Consts.KPI_NAME
        self.templates_to_upload = {}

    @property
    def amz_conn(self):
        if not hasattr(self, '_amz_conn'):
            self._amz_conn = StorageFactory.get_connector(BUCKET)
        return self._amz_conn

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)
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
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
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
        # validate template
        # is_validated = self.validate_template(set_name, data, ignore_missings=ignore_missings)
        # if not is_validated:
        #     os.remove(template_path)
        #     return False
        Log.info(self.log_suffix + 'Template validated successfully')
        # write new KPIs to static tables
        set_static_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]
        if set_static_data.empty:
            Log.info(self.log_suffix + 'Set name does not exist')
            os.remove(template_path)
            return False
        set_fk = set_static_data['kpi_set_fk'].values[0]
        # if not replace_existing:
        #     kpi_names = set_static_data['kpi_name'].unique().tolist()
        #     kpis_to_add = [params for params in data if params.get(self.kpi_name_field) not in kpi_names]
        #     self.add_new_kpi_to_static_tables(set_fk, kpis_to_add)
        #     Log.info(self.log_suffix + '{} KPIs were added'.format(len(kpis_to_add)))
        # else:
        #     kpi_list = set_static_data['kpi_fk'].unique().tolist()
        #     kpi_list = filter(lambda x: isinstance(x, int), kpi_list)
        #     self.delete_kpis(kpi_list)
        #     self.add_new_kpi_to_static_tables(set_fk, data)
        #     Log.info(self.log_suffix + '{} KPIs were deleted and {} were added'.format(len(kpi_list), len(data)))
        self.templates_to_upload[set_name] = template_path
        Log.info(self.log_suffix + 'Template {} is ready to be uploaded'.format(set_name))
        return True

    def upload_new_templates(self, immediate_change=True):
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
            Log.info(self.log_suffix + 'New templates for sets {} were uploaded'.format(self.templates_to_upload))

            for template_name in latest_templates:
                temp_file_path = '{}/{}_temp'.format(os.getcwd(), template_name)
                f = open(temp_file_path, 'wb')
                self.amz_conn.download_file(latest_templates[template_name], f)
                f.close()
                self.amz_conn.save_file(templates_path_in_cloud, template_name, temp_file_path)
                os.remove(temp_file_path)
            Log.info(self.log_suffix + 'Existing templates for sets {} were aligned with the new ones'.format(latest_templates))
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

    def add_new_kpi_to_static_tables(self, set_fk, new_kpi_list):
        """
        :param set_fk: The relevant KPI set FK.
        :param new_kpi_list: a list of all new KPI's parameters.
        This function adds new KPIs to the DB ('Static' table) - both to level2 (KPI) and level3 (Atomic KPI).
        """
        cur = self.rds_conn.db.cursor()
        for kpi in new_kpi_list:
            if kpi:
                # kpi_name = kpi.get(self.kpi_name_field).replace("'", "\\'").encode('utf-8')
                # product_ean_code = str(kpi.get(Consts.PRODUCT_EAN_CODE, kpi.get(Consts.PRODUCT_EAN_CODE2, '')))
                level2_query = """
                               INSERT INTO static.kpi (kpi_set_fk, display_text)
                               VALUES ('{0}', '{1}');""".format(set_fk, kpi)
                cur.execute(level2_query)
                kpi_fk = cur.lastrowid
                level3_query = """
                       INSERT INTO static.atomic_kpi (kpi_fk, name, description, display_text,
                                                      presentation_order, display)
                       VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(kpi_fk,
                                                                                    kpi,
                                                                                    kpi,
                                                                                    kpi,
                                                                                    1, 'Y')
                cur.execute(level3_query)
        self.rds_conn.db.commit()

    def validate_template(self, set_name, data, ignore_missings=False):
        """
        This function goes through the template content and checks whether the data is correct.
        If so, it returns True. Else, it returns False.
        """
        if not data:
            Log.error(self.log_suffix + 'Template for set {} has no data'.format(set_name))
            return False
        fields_of_data = data[0].keys()
        if self.kpi_name_field not in fields_of_data:
            Log.error(self.log_suffix + 'The template for {} must have a {} column, to set the KPI name'.format(set_name, self.kpi_name_field))
            return False
        if set_name in Consts.AVAILABILITY_SETS:
            set_name = 'Availability'
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
                    values = str(params.get(field, '')).split(self.SEPARATOR)
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

    def add_product_kpis_to_static(self):
        cur = self.rds_conn.db.cursor()
        query = """
                select product_ean_code from static.product
                where product_type = 'SKU' and delete_date is null
                """
        cur.execute(query)
        res = cur.fetchall()
        res_list = list(res)
        final_result = [item[0] for item in res_list]

        return final_result


# if __name__ == '__main__':
#     for project_name in ['ccus']:
#         LoggerInitializer.init('{} New Template'.format(project_name))
#         template = NewTemplate(project_name)
#         # products_list = template.add_product_kpis_to_static()
#         # for product in products_list:
#         #     in_ass_kpi_name = str(product) + ' - In Assortment'
#         #     kpi_list.append(in_ass_kpi_name)
#         #     oos_kpi_name = str(product) + ' - OOS'
#         #     kpi_list.append(oos_kpi_name)
#         # template.add_new_kpi_to_static_tables(1, kpi_list)
#         # template.add_dummy_kpi('Visible to Customer')
#         # template.add_kpi_sets_to_static()
#         for set_name in ['Dunkin Donuts']:
#             file_path = '/home/Ortal/Documents/{}.xlsx'.format(set_name)
#             r = template.handle_updated_template(set_name, file_path, ignore_missings=True)
#             if not r:
#                 quit()
#         template.upload_new_templates()

