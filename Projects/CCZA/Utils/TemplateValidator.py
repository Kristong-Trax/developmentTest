from Trax.Tools.ProfessionalServices.Utils.MainTemplate import Main_Template
from Projects.CCZA.Utils.ParseTemplates import parse_template
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Keys import DbUsers
import pandas as pd
import numpy as np
from Projects.CCZA_SAND.Utils.Const import Const


class CczaTemplateValidator(Main_Template):

    def __init__(self, project_name, file_url):
        Main_Template.__init__(self)
        self.project = project_name
        self.template_path = file_url
        self.rds_conn = self.rds_connect
        self.store_data = self.get_store_data()
        # self.all_products = self.get_product_data
        self.kpi_sheets = {}
        self.kpis_lvl2 = self.get_kpis_new_tables()
        self.kpis_old = self.get_kpis_old_tables()
        self.db_static_data = {}
        self.get_static_db_table_contents()

    def get_static_db_table_contents(self):
        # all_tables = map(lambda y: '{}.{}'.format(y[0], y[1]), map(lambda x: x.split('.'),
        #                                                            Parameters.TYPE_DB_MAP.values()))

        for entity, table in Parameters.TYPE_DB_MAP.items():
            table_col = table.split('.')
            table_name = '{}.{}'.format(table_col[0], table_col[1])
            query = """ select * from {} """.format(table_name)
            table_contents = pd.read_sql_query(query, self.rds_conn.db)
            self.db_static_data[entity] = table_contents[table_col[-1]]

    def get_kpis_new_tables(self):
        query = """select * from static.kpi_level_2 """
        kpis = pd.read_sql_query(query, self.rds_conn.db)
        return kpis

    def get_kpis_old_tables(self):
        query = """select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
                   from static.atomic_kpi api
                   left join static.kpi kpi on kpi.pk = api.kpi_fk
                   join static.kpi_set kps on kps.pk = kpi.kpi_set_fk 
                   where kps.name = "Red Score" """
        kpis = pd.read_sql_query(query, self.rds_conn.db)
        return kpis

    @property
    def rds_connect(self):
        self.rds_conn = PSProjectConnector(self.project, DbUsers.Ps)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self.rds_conn.db)
        except Exception as e:
            self.rds_conn.disconnect_rds()
            self.rds_conn = PSProjectConnector(self.project, DbUsers.Ps)
        return self.rds_conn

    def get_store_data(self):
        query = """select * from static.stores where is_active="Y" and delete_date is null and test_store != "Y" """
        store_data = pd.read_sql_query(query, self.rds_conn.db)
        return store_data

    # @property
    # def get_product_data(self):
    #     query = "select * from static.product"
    #     self.all_products = pd.read_sql_query(query, self.rds_conn.db)
    #     return self.store_data

    def validate_template_data(self):
        self.errorHandler.log_info('Checking tabs and columms')
        self.check_all_tabs_exist_and_have_relevant_columns()
        self.errorHandler.log_info('Checking store_types')
        self.check_store_types()
        self.check_kpis_sheets()

        # validate sheets contents against sheet contents
        # validate kpi names in the db both against old tables and new tables
        # validate specific template parameters
        # validate all weights and targets are aligned
        #
        pass

    def check_kpis_sheets(self):
        for sheet, template_df in self.kpi_sheets.items():
            self.perform_configurable_validations()
            self.perform_additional_validations()
        pass

    def perform_configurable_validations(self):
        pass

    def perform_additional_validations(self):
        pass

    def check_all_tabs_exist_and_have_relevant_columns(self):
        for name in Const.sheet_names_and_rows:
            try:
                template_df = parse_template(self.template_path, sheet_name=name,
                                                       lower_headers_row_index=Const.sheet_names_and_rows[name])
                columns = template_df.columns.values
                columns = filter(lambda x: 'Unnamed' not in x, columns)
                template_df = template_df[columns]
                self.check_template_columns(name, columns)
                self.kpi_sheets[name] = template_df
            except Exception as e: # look up the type of exception in case sheet name is missing
                self.errorHandler.lgiog_error('Sheet {} is missing in the file'.format(name))

    def check_template_columns(self, sheet, columns):
        missing_columns = filter(lambda x: x not in columns, Parameters.SHEETS_COL_MAP[sheet])
        if len(missing_columns) > 0:
            self.errorHandler.log_error('The following columns are missing '
                                        'from sheet {}: {}'.format(sheet, missing_columns))

    def check_store_types(self):
        for sheet, template_df in self.kpi_sheets.items():
            store_types = set(template_df.columns.values).difference(set(Parameters.SHEETS_COL_MAP[sheet]))
            store_types_in_db = set(self.store_data[Const.ATTR3].values)
            template_vs_db = store_types.difference(store_types_in_db)
            if len(template_vs_db) > 0:
                self.errorHandler.log_error('Store types {} exist in template but do not '
                                            'exist in DB for sheet {}'.format(template_vs_db, sheet))
            db_vs_template = store_types_in_db.difference(store_types)
            if len(db_vs_template) > 0:
                self.errorHandler.log_error('Store types {} exist in db but do not '
                                            'exist in the template for sheet {}'.format(db_vs_template, sheet))

    # simple validations
    def check_list(self):
        pass

    def check_db_values(self):
        pass

    def check_column_vs_column(self, sheet_col_1, sheet_col_2):
        pass

    def check_empty_values(self):
        pass

    # complex validations

class Parameters(object):
    ALL = 'ALL'
    TARGET_TABS = [Const.LIST_OF_ENTITIES, Const.SOS_TARGETS, Const.PRICING_TARGETS, Const.SURVEY_QUESTIONS,
                   Const.FLOW_PARAMETERS]
    TYPE_DB_MAP = {'SKU': 'static_new.product.ean_code', 'EAN': 'static_new.product.ean_code',
                   'Brand': 'static_new.brand.name', 'Category': 'static_new.category.name',
                   'Manufacturer': 'static_new.manufacturer.name',  'Sub_category': 'static_new.sub_category.name',
                   'Product_type': 'static_new.product.type', 'Template Name': 'static.template.name',
                   'Survey': 'static.survey_question.code', 'Location Types': 'static.location_types'}
    # TYPE_PROPERTY_MAP = {'SKU': 'product.ean_code', 'EAN': 'product.ean_code',
    #                      'Brand': 'static_new.brand.name', 'Category': 'static_new.category.name',
    #                       'Manufacturer': 'static_new.manufacturer.name',  'Sub_category': 'static_new.sub_category.name',
    #                       'Product_type': 'static_new.product.type', 'Template Name': 'static.template.name',
    #                       'Survey': 'static.survey_question.code'}

    WEIGHT_TABS = [Const.SOS_WEIGHTS, Const.PRICING_WEIGHTS]
    SHEETS_COL_MAP = {Const.KPIS: [Const.KPI_NAME, Const.KPI_GROUP, Const.KPI_TYPE, 'Tested KPI Group',
                                   Const.TARGET, Const.WEIGHT_SHEET, 'SCORE'],
                      Const.LIST_OF_ENTITIES: [Const.KPI_NAME, Const.ATOMIC_NAME, Const.ENTITY_TYPE, Const.ENTITY_VAL,
                                               Const.ENTITY_TYPE2, Const.ENTITY_VAL2, Const.IN_NOT_IN,
                                               Const.TYPE_FILTER, Const.VALUE_FILTER],
                      Const.SOS_WEIGHTS: [Const.KPI_NAME, Const.ATOMIC_NAME, Const.ENTITY_TYPE_NUMERATOR,
                                          Const.NUMERATOR, Const.ENTITY_TYPE_DENOMINATOR, Const.DENOMINATOR,
                                          Const.IN_NOT_IN, Const.TYPE_FILTER, Const.VALUE_FILTER, Const.SCORE],
                      Const.SOS_TARGETS: [Const.KPI_NAME, Const.ATOMIC_NAME],
                      Const.PRICING_WEIGHTS: [Const.KPI_NAME, Const.ATOMIC_NAME, Const.SURVEY_Q_CODE],
                      Const.PRICING_TARGETS: [Const.KPI_NAME, Const.ATOMIC_NAME, Const.SURVEY_Q_ID],
                      Const.SURVEY_QUESTIONS: [Const.KPI_NAME, Const.ATOMIC_NAME, Const.ENTITY_TYPE, Const.ENTITY_VAL,
                                               Const.ENTITY_TYPE2, Const.ENTITY_VAL2, Const.IN_NOT_IN,
                                               Const.TYPE_FILTER, Const.VALUE_FILTER, Const.SURVEY_Q_CODE,
                                               Const.ACCEPTED_ANSWER_RESULT],
                      Const.FLOW_PARAMETERS: [Const.KPI_NAME, Const.ATOMIC_NAME, Const.ENTITY_TYPE, Const.ENTITY_VAL,
                                              Const.IN_NOT_IN, Const.TYPE_FILTER, Const.VALUE_FILTER]}

    SHEETS_COL_VALID_TYPE = {
        ALL: {
            Const.KPI_NAME: {'type': ('prop', 'prop'), 'source': ('kpis_lvl2.name', 'kpis_old.kpi_name'),
                             'disallow_empty': True, 'filter_out': [Const.RED_SCORE]},
            Const.ATOMIC_NAME: {'type': ('prop', 'prop'), 'source': ('kpis_lvl2.name', 'kpis_old.atomic_kpi_name'),
                                'disallow_empty': True},
            Const.ENTITY_TYPE: {'type': ('list',), 'source': (TYPE_DB_MAP.keys(),),
                                'disallow_empty': False},
            Const.ENTITY_VAL: {'type': ('type_value',), 'source': ({'db': TYPE_DB_MAP, 'column': Const.ENTITY_TYPE}),
                               'disallow_empty': False},
            Const.ENTITY_TYPE2: {'type': ('list',), 'source': (TYPE_DB_MAP.keys(),),
                                'disallow_empty': False},
            Const.ENTITY_VAL2: {'type': ('type_value',), 'source': ({'db': TYPE_DB_MAP, 'column': Const.ENTITY_TYPE2}),
                               'disallow_empty': False},
            Const.IN_NOT_IN: {'type': ('list',), 'source': (['In', 'Not in']),
                              'disallow_empty': False},
            Const.TYPE_FILTER: {'type': ('list',), 'source': (TYPE_DB_MAP.keys(),),
                                'disallow_empty': False},
            Const.VALUE_FILTER: {'type': ('type_value',), 'source': ({'db': TYPE_DB_MAP, 'column': Const.TYPE_FILTER}),
                                 'disallow_empty': False}
        },
        Const.KPIS: {
            # Const.KPI_NAME: {'type': ('prop', 'prop'),'source': ('kpis_lvl2.name', 'kpis_old.kpi_name'),
            #                           'disallow_empty': True, 'filter_out': [Const.RED_SCORE]},
            Const.KPI_GROUP: {'type': ('list',), 'source': ([Const.RED_SCORE]), 'disallow_empty': False},
            Const.KPI_TYPE: {'type': ('list',), 'source': ([Const.AVAILABILITY, Const.SOS_FACINGS,
                                                            Const.SURVEY_QUESTION, Const.FLOW]),
                                      'disallow_empty': True, 'filter_out': ['Weighted SUM KPIs in GROUP']},
            Const.TARGET: {'type': ('list',),'source': (TARGET_TABS, ), 'disallow_empty': False},
            Const.WEIGHT_SHEET: {'type': ('list',), 'source': (WEIGHT_TABS, ), 'disallow_empty': False}
        },
        Const.LIST_OF_ENTITIES: {},
        Const.SOS_WEIGHTS: {
            Const.ENTITY_TYPE_NUMERATOR: {'type': ('list',), 'source': (TYPE_DB_MAP.keys(),),
                                          'disallow_empty': False},
            Const.NUMERATOR: {'type': ('type_value',),
                              'source': ({'db': TYPE_DB_MAP, 'column': Const.ENTITY_TYPE_NUMERATOR}),
                              'disallow_empty': False},
            Const.ENTITY_TYPE_DENOMINATOR: {'type': ('list',), 'source': (TYPE_DB_MAP.keys(),),
                                            'disallow_empty': False},
            Const.DENOMINATOR: {'type': ('type_value',),
                              'source': ({'db': TYPE_DB_MAP, 'column': Const.ENTITY_TYPE_DENOMINATOR}),
                              'disallow_empty': False},
            Const.SCORE: {'type': ('list',), 'source': (['numeric', 'binary']),
                          'disallow_empty': True},
        },
        Const.SOS_TARGETS: {},
        Const.PRICING_WEIGHTS: {
            Const.SURVEY_Q_CODE: {'type': ('db',), 'source': ('static.survey_question.code',),
                                  'disallow_empty': True},
        },
        Const.PRICING_TARGETS: {
            Const.SURVEY_Q_ID: {'type': ('db',), 'source': ('static.survey_question.code',),
                                'disallow_empty': True},
        },
        Const.SURVEY_QUESTIONS: {
            Const.ACCEPTED_ANSWER_RESULT: {'disallow_empty': True, 'filter_out': [Const.AVAILABILITY]},
            Const.KPI_TYPE: {'type': ('list',), 'source': ([Const.AVAILABILITY, Const.SCENE_COUNT, Const.SURVEY,
                                                            Const.PLANOGRAM]),
                             'disallow_empty': True},
        },
        Const.FLOW_PARAMETERS: {
            Const.KPI_TYPE: {'type': ('list',), 'source': ([Const.FLOW],),
                             'disallow_empty': False},
        }

    }
