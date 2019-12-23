from Trax.Tools.ProfessionalServices.Utils.MainTemplate import Main_Template
from Projects.CCZA.Utils.ParseTemplates import parse_template
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Keys import DbUsers
import pandas as pd
import numpy as np
import os
from Projects.CCZA_SAND.Utils.Const import Const
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Tools.ProfessionalServices.Utils.Util import Utilities

DEFAULT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


class CczaTemplateValidator(Main_Template):

    def __init__(self, project_name, file_url=DEFAULT_PATH):
        Main_Template.__init__(self)
        self.project = project_name
        self.template_path = file_url
        self.rds_conn = self.rds_connect
        self.store_data = self.get_store_data()
        self.store_types_db = self.store_data[Const.ATTR3].values.tolist()
        # self.all_products = self.get_product_data
        self.kpi_sheets = {}
        self.kpis_lvl2 = self.get_kpis_new_tables()
        self.kpis_old = self.get_kpis_old_tables()
        self.db_static_data = {}
        self.get_static_db_table_contents()
        self.type_function_map = self.map_validation_function_to_valid_type()

    def get_static_db_table_contents(self):
        # all_tables = map(lambda y: '{}.{}'.format(y[0], y[1]), map(lambda x: x.split('.'),
        #                                                            Parameters.TYPE_DB_MAP.values()))
        table_data = {}

        for entity, table in Parameters.TYPE_DB_MAP.items():
            table_col = table.split('.')
            table_name = '{}.{}'.format(table_col[0], table_col[1])
            if table_data.get(table_name) is None:
                query = """ select * from {} """.format(table_name)
                table_contents = pd.read_sql_query(query, self.rds_conn.db)
                table_data.update({table_name: table_contents})
            else:
                table_contents = table_data.get(table_name)
            self.db_static_data[entity] = table_contents[table_col[-1]]

        for templ_column, table in Parameters.COLUMN_DB_MAP.items():
            table_col = table.split('.')
            table_name = '{}.{}'.format(table_col[0], table_col[1])
            if table_data.get(table_name) is None:
                query = """ select * from {} """.format(table_name)
                table_contents = pd.read_sql_query(query, self.rds_conn.db)
                table_data.update({table_name: table_contents})
            else:
                table_contents = table_data.get(table_name)
            self.db_static_data[templ_column] = table_contents[table_col[-1]]

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
        try:
            self.errorHandler.log_info('Checking tabs and columns')
            self.check_all_tabs_exist_and_have_relevant_columns()
        except Exception as e:
            self.errorHandler.log_error('Unhandled error in format checking: {}'.format(e))

        try:
            self.errorHandler.log_info('Checking store_types')
            self.check_store_types_and_extra_columns()
        except Exception as e:
            self.errorHandler.log_error('Unhandled error in store type validation: {}'.format(e))

        try:
            self.errorHandler.log_info('Checking sheets data')
            self.check_kpis_sheets()
        except Exception as e:
            self.errorHandler.log_error('Unhandled error in sheet data validation: {}'.format(e))
        error_file_link = self.dump_logs_to_file_and_upload_to_bucket()
        print error_file_link
        return

    def dump_logs_to_file_and_upload_to_bucket(self):
        file_path = self.errorHandler.dump_to_file()
        error_file_link = Utilities.upload_file_to_s3(file_path, '{}.log'.format(self.execution_unique_id),
                                                      os.path.join(Parameters.BUCKET_FOLDER,
                                                                   Config.get_environment().upper(),
                                                                   self.project.upper()),
                                                      Parameters.LOG_FILE_BUCKET)
        return error_file_link

    def check_kpis_sheets(self):
        for sheet, template_df in self.kpi_sheets.items():
            self.errorHandler.log_info('Validating Sheet: {}'.format(sheet))
            self.errorHandler.log_info('Checking configurable validations')
            self.perform_configurable_validations(sheet, template_df)
            self.errorHandler.log_info('Checking additional validations')
            self.perform_additional_validations(sheet, template_df)

    def perform_configurable_validations(self, sheet, template_df):
        columns_to_validate = filter(lambda x: x in Parameters.SHEETS_COL_MAP[sheet], template_df.columns.values)
        for templ_column in columns_to_validate:
            self.errorHandler.log_info('Validating sheet {}, column: {}'.format(sheet, templ_column))
            validation_params = Parameters.SHEETS_COL_VALID_TYPE[Parameters.ALL][templ_column] if \
                templ_column in Parameters.SHEETS_COL_VALID_TYPE[Parameters.ALL].keys() else \
                Parameters.SHEETS_COL_VALID_TYPE[sheet][templ_column]

            if validation_params.get('filter_out'):
                filtering_param = validation_params.get('filter_out')
                template_df = template_df[~(template_df[templ_column].isin(filtering_param))]

            self.validate_empty(sheet, template_df, templ_column, validation_params)
            self.validate_particular_values(sheet, template_df, templ_column, validation_params)

    def validate_particular_values(self, sheet, template_df, templ_column, validation_params):
        val_types = validation_params.get('type')
        val_sources = validation_params.get('source')
        if val_types is not None:
            for i in range(len(val_types)):
                val_type = val_types[i]
                val_source = val_sources[i]
                self.type_function_map[val_type](sheet, template_df, templ_column, val_source)

    def check_value_based_on_type(self, sheet, template_df, templ_column, val_source):
        type_source_col = val_source['column']
        empty_type_records = template_df[(template_df[type_source_col] == '') |
                                         (template_df[type_source_col].isnull())]
        if len(empty_type_records) > 0:
            self.errorHandler.log_error('Sheet: {}. Column: {}. Entity types are not completed for kpis: '
                                        '{}'.format(sheet, type_source_col, empty_type_records[Const.ATOMIC_NAME].values))

        sheet_df = template_df[[type_source_col, templ_column]]
        sheet_df = sheet_df[(~(sheet_df[type_source_col].isnull())) &
                            (~(sheet_df[type_source_col] == ''))]
        sheet_df = sheet_df[(~(sheet_df[templ_column].isnull())) &
                            (~(sheet_df[templ_column] == ''))]

        # handle double entity
        double_entity_df = sheet_df[sheet_df[type_source_col].str.contains(',')]
        if not double_entity_df.empty:
            missing_values_df = double_entity_df[~(double_entity_df[templ_column].str.contains(','))]
            if len(missing_values_df) > 0:
                self.errorHandler.log_error('Sheet {}. One of multiple values is missing '
                                            'in column {}'.format(sheet, templ_column))
            double_entity_df = double_entity_df[double_entity_df[templ_column].str.contains(',')]
            duplicate_df = double_entity_df.copy()
            double_entity_df[type_source_col] = double_entity_df[type_source_col].apply(lambda x: x.split(',')[0].strip())
            double_entity_df[templ_column] = double_entity_df[templ_column].apply(lambda x: x.split(',')[0].strip())
            duplicate_df[type_source_col] = duplicate_df[type_source_col].apply(lambda x: x.split(',')[1].strip())
            duplicate_df[templ_column] = duplicate_df[templ_column].apply(lambda x: x.split(',')[1].strip())
            joined_template_df = pd.concat([double_entity_df, duplicate_df])
            self.check_value_based_on_type(sheet, joined_template_df, templ_column, val_source)

        # handle single entity
        sheet_df = sheet_df[~(sheet_df[type_source_col].str.contains(','))]
        sheet_df = sheet_df.sort_values(by=[type_source_col])
        sheet_df['accumulated_values'] = sheet_df.groupby(type_source_col)[templ_column].apply(
            lambda x: (x + ',').cumsum().str.strip())
        sheet_df = sheet_df.drop_duplicates(subset=[type_source_col], keep='last')
        sheet_df = sheet_df.reset_index(drop=True)

        for i, row in sheet_df.iterrows():
            validated_entity = row[type_source_col]
            template_values = row['accumulated_values'][0:-1].split(',')
            template_values = set(map(lambda z: z.strip(), template_values))

            db_col = val_source['db'][validated_entity].split('.')[-1]
            db_values = set(self.db_static_data[validated_entity].values)
            diff = template_values.difference(db_values)
            if len(diff) > 0:
                self.errorHandler.log_error('Sheet: {}, Column: {}, Entity: {} '
                                            'does not match DB: {}'.format(sheet, templ_column, validated_entity, diff))

    def check_value_based_on_property(self, sheet, template_df, templ_column, val_source):
        prop_name, col = val_source.split('.')
        # source_values = set(self.__getattribute__(prop_name)[col].values)
        source_values = set(getattr(self, prop_name)[col].values)
        template_values = filter(lambda x: x == x or x != '' or x is not None, template_df[templ_column].values)
        template_values = set(template_values)
        diff = template_values.difference(source_values)
        if len(diff) > 0:
            self.errorHandler.log_error('{} values do not match {} of {} '
                                        'in sheet {}: {}'.format(templ_column, col, prop_name, sheet, diff))

    def check_db_values(self, sheet, template_df, templ_column, val_source):
        db_col = val_source.split('.')[-1]
        db_col_values = filter(lambda x: x == x and x != '' and x is not None, self.db_static_data[templ_column].values)
        db_col_values = set(db_col_values)
        template_values = filter(lambda x: x == x and x != '' and x is not None, template_df[templ_column].values)
        template_values = set(template_values)
        diff = template_values.difference(db_col_values)
        if len(diff) > 0:
            try:
                template_values1 = set(map(lambda x: float(x), list(template_values)))
                db_col_values1 = set(map(lambda x: float(x), list(db_col_values)))
                diff1 = template_values1.difference(db_col_values1)
                if len(diff1) > 0:
                    self.errorHandler.log_error('Sheet {}: . Column: {}. Values do not match values of'
                                                ' column {} in DB table {}: '
                                                '{}'.format(sheet, templ_column, db_col, val_source, diff))
            except Exception:
                self.errorHandler.log_error('Sheet {}: . Column: {}. Values do not match values of'
                                            ' column {} in DB table {}: '
                                            '{}'.format(sheet, templ_column, db_col, val_source, diff))

    def validate_list(self, sheet, template_df, templ_column, val_source):
        template_values = filter(lambda x: x == x and x != '' and x is not None, template_df[templ_column].values)
        template_values_final = []
        for raw_value in template_values:
            values = map(lambda x: x.strip(), raw_value.split(','))
            for value in values:
                template_values_final.append(value)
        template_values = set(template_values_final)
        val_values = set(val_source)
        diff = template_values.difference(val_values)
        if len(diff) > 0:
            self.errorHandler.log_error('Values in column {} in sheet {} do not match the allowed values {}: '
                                        '{}'.format(templ_column, sheet, val_values, diff))

    def validate_empty(self, sheet, template_df, templ_column, validation_params):
        if validation_params.get('disallow_empty'):
            empty_values = filter(lambda x: (x == '') or (x is None) or (x != x),  template_df[templ_column].values)
            if len(empty_values) > 0:
                self.errorHandler.log_error('Column {} in sheet {} has empty values'.format(templ_column, sheet))

    def perform_additional_validations(self, sheet, template_df):
        if sheet == Const.KPIS:
            self.validate_weights_in_kpis_sheet(sheet, template_df)
        if sheet != Const.KPIS:
            self.check_duplicate_atomics(sheet, template_df)
        if sheet in Parameters.TARGET_SHEET_WEIGHT_SHEET_MAP.keys():
            self.compare_weights_vs_targets_sheet(sheet, template_df)
        if sheet in [Const.LIST_OF_ENTITIES, Const.SOS_WEIGHTS, Const.PRICING_WEIGHTS, Const.SURVEY_QUESTIONS]:
            self.check_weights_for_lvl2_kpis_add_to_100(sheet, template_df)

    def check_weights_for_lvl2_kpis_add_to_100(self, sheet, template_df):
        non_store_columns = list(filter(lambda x: x in Parameters.SHEETS_COL_MAP[sheet], template_df.columns.values))
        non_store_columns.remove(Const.KPI_NAME)
        template_df = template_df.drop(non_store_columns, axis=1)
        groupby_dict = {}
        store_col = filter(lambda x: x != Const.KPI_NAME, template_df.columns.values)
        template_df[store_col] = template_df[store_col].replace('', 0.0)
        template_df[store_col] = template_df[store_col].astype(float)
        for col in store_col:
            groupby_dict.update({col: np.sum})
        aggregate_df = template_df.groupby([Const.KPI_NAME], as_index=False).agg(groupby_dict)
        for i, row in aggregate_df.iterrows():
            all_100 = all(map(lambda x: x == 100 or x == 1, row[store_col].values))
            if not all_100:
                self.errorHandler.log_error('Sheet {}. KPI Name: {} . Not '
                                            'all weights per stores add up to 100'.format(sheet, row[Const.KPI_NAME]))

    def compare_weights_vs_targets_sheet(self, target_sheet, targets_df):
        weight_sheet = Parameters.TARGET_SHEET_WEIGHT_SHEET_MAP[target_sheet]
        weights_df = self.kpi_sheets[weight_sheet]
        self.compare_kpi_lists(target_sheet, targets_df, weight_sheet, weights_df)
        self.compare_weights_and_targets_store_sections(target_sheet, targets_df, weight_sheet, weights_df)

    def compare_weights_and_targets_store_sections(self, target_sheet, targets_df, weight_sheet, weights_df):
        # target_non_store_columns = filter(lambda x: x in Parameters.SHEETS_COL_MAP[target_sheet],
        #                                   targets_df.columns.values)
        # target_stores_df = targets_df.drop(target_non_store_columns, axis=1)
        if Const.KPI_NAME in targets_df.columns.values and Const.ATOMIC_NAME in targets_df.columns.values:
            targets_df = targets_df.sort_values([Const.KPI_NAME, Const.ATOMIC_NAME])
            existing_store_types = filter(lambda x: x in self.store_types_db, targets_df.columns.values)
            target_stores_df = targets_df[existing_store_types]
            target_stores_df = target_stores_df.sort_index(axis=1)
            store_columns_t = target_stores_df.columns.values
            # store_columns_t.sort()
            # target_stores_df = target_stores_df[list(store_columns_t)]
            target_values = target_stores_df.values
            target_values = target_values.astype(np.bool)

            # weight_non_store_columns = filter(lambda x: x in Parameters.SHEETS_COL_MAP[weight_sheet],
            #                                   targets_df.columns.values)
            # weight_stores_df = weights_df.drop(weight_non_store_columns, axis=1)

            weights_df = weights_df.sort_values([Const.KPI_NAME, Const.ATOMIC_NAME])
            existing_store_types = filter(lambda x: x in self.store_types_db, weights_df.columns.values)
            weight_stores_df = weights_df[existing_store_types]
            weight_stores_df = weight_stores_df.sort_index(axis=1)
            store_columns_w = weight_stores_df.columns.values
            # store_columns_w.sort()
            # weight_stores_df = weight_stores_df[store_columns_w]
            weight_values = weight_stores_df.values
            weight_values = weight_values.astype(np.bool)
            if np.array_equal(store_columns_t, store_columns_w) and target_values.shape == weight_values.shape:
                compare = np.isclose(target_values, weight_values)
                compare_df = pd.DataFrame(compare, columns=store_columns_t)
                if not compare.all():
                    self.errorHandler.log_error('weights and targets are not aligned in sheets'
                                                ' {} and {}'.format(target_sheet, weight_sheet))
                    for col in compare_df.columns.values:
                        compare_col_res = all(compare_df[col].values.tolist())
                        if not compare_col_res:
                            self.errorHandler.log_error('Sheets: {} and {}. Fix weights or targets '
                                                        'for store {}:'.format(weight_sheet, target_sheet, col))
            else:
                self.errorHandler.log_error('Stores or kpi lists are not the same in '
                                            'sheets {} and {}'.format(target_sheet, weight_sheet))

    def compare_kpi_lists(self, target_sheet, targets_df, weight_sheet, weights_df):
        weights_df = weights_df[[Const.KPI_NAME, Const.ATOMIC_NAME]]
        weights_df.rename(columns={Const.KPI_NAME: 'KPI_Name_Weights'}, inplace=True)
        targets_df = targets_df[[Const.KPI_NAME, Const.ATOMIC_NAME]]
        targets_df.rename(columns={Const.KPI_NAME: 'KPI_Name_Targets'}, inplace=True)
        validation_df = pd.merge(targets_df, weights_df, on=[Const.ATOMIC_NAME], how='outer')
        missing_weights = validation_df[validation_df['KPI_Name_Targets'].isnull()]
        if len(missing_weights) > 0:
            atomics = missing_weights[Const.ATOMIC_NAME].values.tolist()
            self.errorHandler.log_error('Sheet: {} has atomics that are missing '
                                        'from Sheet {}: {}'.format(target_sheet, weight_sheet, atomics))
        missing_targets = validation_df[validation_df['KPI_Name_Weights'].isnull()]
        if len(missing_targets) > 0:
            atomics = missing_weights[Const.ATOMIC_NAME].values.tolist()
            self.errorHandler.log_error('Sheet: {} has atomics that are missing '
                                        'from Sheet {}: {}'.format(weight_sheet, target_sheet, atomics))

    def check_duplicate_atomics(self, sheet, template_df):
        atomics_df = template_df[[Const.ATOMIC_NAME]]
        atomics_df['count'] = 1
        atomics_df = atomics_df.groupby([Const.ATOMIC_NAME], as_index=False).agg({'count': np.sum})
        atomics_df = atomics_df[atomics_df['count']>1]
        for i, row in atomics_df.iterrows():
            self.errorHandler.log_error('Sheet {} has duplicate atomic kpi rows for '
                                        'kpi {}'.format(sheet, row[Const.ATOMIC_NAME]))

    def validate_weights_in_kpis_sheet(self, sheet, template_df):
        # store_weights_df = template_df.drop(Parameters.SHEETS_COL_MAP[sheet], axis=1)
        existing_store_types = filter(lambda x: x in self.store_types_db, template_df.columns.values)
        store_weights_df = template_df[existing_store_types]
        store_weights = store_weights_df.values
        try:
            store_weights.astype(float)
        except (ValueError, TypeError):
            self.errorHandler.log_error('Sheet: {}. Not all weights in KPIs sheet are filled or '
                                        'are numeric'.format(sheet))
        total_weights = store_weights[store_weights.shape[0]-1:store_weights.shape[0]][0].astype(float)
        validate_100 = all(map(lambda x: x == 100, total_weights))
        if not validate_100:
            self.errorHandler.log_error('Sheet: {}. Certain weights per store type are not equal to 100'.format(sheet))

        total_weights_summed = store_weights[0:-1].astype(float)
        total_weights_summed = total_weights_summed.sum(axis=0)
        validate_100 = all(map(lambda x: x == 100, total_weights_summed))
        if not validate_100:
            self.errorHandler.log_error('Sheet: {}. Certain weights per store type do not add up to 100'.format(sheet))

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
                self.errorHandler.log_error('Sheet {} is missing in the file'.format(name))

    def check_template_columns(self, sheet, columns):
        missing_columns = filter(lambda x: x not in columns, Parameters.SHEETS_COL_MAP[sheet])
        if len(missing_columns) > 0:
            self.errorHandler.log_error('The following columns are missing '
                                        'from sheet {}: {}'.format(sheet, missing_columns))

    def check_store_types_and_extra_columns(self):
        for sheet, template_df in self.kpi_sheets.items():
            store_types = set(template_df.columns.values).difference(set(Parameters.SHEETS_COL_MAP[sheet]))
            store_types_in_db = set(self.store_data[Const.ATTR3].values)
            template_vs_db = store_types.difference(store_types_in_db)
            if len(template_vs_db) > 0:
                self.errorHandler.log_error('Sheet: {}. Store types {} exist in template but do not '
                                            'exist in DB / or extra columns exist. Only data related to stores types '
                                            'existing in DB will be further validated'.format(sheet, template_vs_db))
            db_vs_template = store_types_in_db.difference(store_types)
            if len(db_vs_template) > 0:
                self.errorHandler.log_error('Sheet: {}. Store types {} exist in db but do not '
                                            'exist in the template / or extra columns exist'.format(sheet, template_vs_db))

    def map_validation_function_to_valid_type(self):
        type_func_map = {'db': self.check_db_values, 'list': self.validate_list,
                         'type_value': self.check_value_based_on_type, 'prop': self.check_value_based_on_property}
        return type_func_map


class Parameters(object):
    BUCKET_FOLDER = 'PS_CUSTOM_METADATA'
    LOG_FILE_BUCKET = 'traxusapi'
    ALL = 'ALL'
    TARGET_TABS = [Const.LIST_OF_ENTITIES, Const.SOS_TARGETS, Const.PRICING_TARGETS, Const.SURVEY_QUESTIONS,
                   Const.FLOW_PARAMETERS]
    TYPE_DB_MAP = {'SKU': 'static_new.product.ean_code', 'EAN': 'static_new.product.ean_code',
                   'Brand': 'static_new.brand.name', 'Category': 'static_new.category.name',
                   'Manufacturer': 'static_new.manufacturer.name',  'Sub_category': 'static_new.sub_category.name',
                   'Product_type': 'static_new.product.type', 'Template Name': 'static.template.name',
                   'Survey': 'static.survey_question.code', 'Location Types': 'static.location_types.name'}
    COLUMN_DB_MAP = {Const.SURVEY_Q_CODE:  'static.survey_question.code',
                     Const.SURVEY_Q_ID: 'static.survey_question.code'}


    WEIGHT_TABS = [Const.SOS_WEIGHTS, Const.PRICING_WEIGHTS]
    SHEETS_COL_MAP = {Const.KPIS: [Const.KPI_NAME, Const.KPI_GROUP, Const.KPI_TYPE,
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
                      Const.SURVEY_QUESTIONS: [Const.KPI_NAME, Const.ATOMIC_NAME, Const.KPI_TYPE, Const.ENTITY_TYPE, Const.ENTITY_VAL,
                                               Const.ENTITY_TYPE2, Const.ENTITY_VAL2, Const.IN_NOT_IN,
                                               Const.TYPE_FILTER, Const.VALUE_FILTER, Const.SURVEY_Q_CODE,
                                               Const.ACCEPTED_ANSWER_RESULT],
                      Const.FLOW_PARAMETERS: [Const.KPI_NAME, Const.ATOMIC_NAME, Const.ENTITY_TYPE, Const.ENTITY_VAL,
                                              Const.IN_NOT_IN, Const.TYPE_FILTER, Const.VALUE_FILTER, Const.KPI_TYPE]}
    TARGET_SHEET_WEIGHT_SHEET_MAP = {Const.SOS_TARGETS: Const.SOS_WEIGHTS, Const.PRICING_TARGETS: Const.PRICING_WEIGHTS}

    SHEETS_COL_VALID_TYPE = {
        ALL: {
            Const.KPI_NAME: {'type': ('prop', 'prop'), 'source': ('kpis_lvl2.type', 'kpis_old.kpi_name'),
                             'disallow_empty': True, 'filter_out': [Const.RED_SCORE]},
            Const.ATOMIC_NAME: {'type': ('prop', 'prop'), 'source': ('kpis_lvl2.type', 'kpis_old.atomic_kpi_name'),
                                'disallow_empty': True},
            Const.ENTITY_TYPE: {'type': ('list',), 'source': (TYPE_DB_MAP.keys(),),
                                'disallow_empty': False},
            Const.ENTITY_VAL: {'type': ('type_value',), 'source': ({'db': TYPE_DB_MAP, 'column': Const.ENTITY_TYPE},),
                               'disallow_empty': False},
            Const.ENTITY_TYPE2: {'type': ('list',), 'source': (TYPE_DB_MAP.keys(),),
                                 'disallow_empty': False},
            Const.ENTITY_VAL2: {'type': ('type_value',), 'source': ({'db': TYPE_DB_MAP, 'column': Const.ENTITY_TYPE2},),
                                'disallow_empty': False},
            Const.IN_NOT_IN: {'type': ('list',), 'source': (['In', 'Not in'],),
                              'disallow_empty': False},
            Const.TYPE_FILTER: {'type': ('list',), 'source': (TYPE_DB_MAP.keys(),),
                                'disallow_empty': False},
            Const.VALUE_FILTER: {'type': ('type_value',), 'source': ({'db': TYPE_DB_MAP, 'column': Const.TYPE_FILTER},),
                                 'disallow_empty': False}
        },
        Const.KPIS: {
            'SCORE': {'type': ('list',), 'source': (['numeric', 'binary'],),
                                    'disallow_empty': True},
            Const.KPI_GROUP: {'type': ('list',), 'source': ([Const.RED_SCORE],), 'disallow_empty': False},
            Const.KPI_TYPE: {'type': ('list',), 'source': ([Const.AVAILABILITY, Const.SOS_FACINGS,
                                                            Const.SURVEY_QUESTION, Const.FLOW],),
                                      'disallow_empty': True, 'filter_out': ['Weighted SUM KPIs in GROUP']},
            Const.TARGET: {'type': ('list',),'source': (TARGET_TABS, ), 'disallow_empty': False},
            Const.WEIGHT_SHEET: {'type': ('list',), 'source': (WEIGHT_TABS, ), 'disallow_empty': False}
        },
        Const.LIST_OF_ENTITIES: {},
        Const.SOS_WEIGHTS: {
            Const.ENTITY_TYPE_NUMERATOR: {'type': ('list',), 'source': (TYPE_DB_MAP.keys(),),
                                          'disallow_empty': False},
            Const.NUMERATOR: {'type': ('type_value',),
                              'source': ({'db': TYPE_DB_MAP, 'column': Const.ENTITY_TYPE_NUMERATOR},),
                              'disallow_empty': False},
            Const.ENTITY_TYPE_DENOMINATOR: {'type': ('list',), 'source': (TYPE_DB_MAP.keys(),),
                                            'disallow_empty': False},
            Const.DENOMINATOR: {'type': ('type_value',),
                                'source': ({'db': TYPE_DB_MAP, 'column': Const.ENTITY_TYPE_DENOMINATOR},),
                                'disallow_empty': False},
            Const.SCORE: {'type': ('list',), 'source': (['numeric', 'binary'],),
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
            Const.ACCEPTED_ANSWER_RESULT: {'disallow_empty': False},
            Const.KPI_TYPE: {'type': ('list',), 'source': ([Const.AVAILABILITY, Const.SCENE_COUNT, Const.SURVEY,
                                                            Const.PLANOGRAM],),
                             'disallow_empty': True},
            Const.SURVEY_Q_CODE: {'type': ('db',), 'source': ('static.survey_question.code',),
                                  'disallow_empty': False},
        },
        Const.FLOW_PARAMETERS: {
            Const.KPI_TYPE: {'type': ('list',), 'source': ([Const.FLOW],),
                             'disallow_empty': False},
        }

    }


"""
project_name: ccza or ccza
file_url: optional attribute. Path to the template that needs to be validated. 
         By default, it takes the path to the template used by the code in PROD

Validation types performed: 
1. Template format validation: checks that all tabs and columns are present in the template
2. Store types: check that the store types (attribute_3) in the template are compliant with the DB
3. Data in each particular sheet:
        3.1. Configurable Validations: validations that are configured per template sheet and columns in Parameters class
        3.2. Additional Validations: validations that are necessary based on the practice of using the template:
            a. Sheet 'KPIs': checks that all weights exist for all stores and that the Red Score line is 100
            b. Sheets except 'KPIs': check that there are no duplicate atomics on the sheet
            c. Sheets that have both targets and weights: 
                    - checks that the lists of kpis match, 
                    - checks that if there are targets per store type the weights also exist for this store_type
                    - checks that weights for lvl 2 kpis add up to 100 for each store type
            
"""

if __name__ == '__main__':
    LoggerInitializer.init('ccza calculations')
    Config.init()
    project_name = 'ccza'
    file_path = '/home/natalyak/Desktop/CCZA/PS KPI Template JANUARY 2020.xlsx'
    validator = CczaTemplateValidator(project_name=project_name, file_url=file_path)
    validator.validate_template_data()