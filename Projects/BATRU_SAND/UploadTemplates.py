# -*- coding: utf-8 -*-

import os
import pandas as pd
import getpass
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Conf.Configuration import Config
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.BATRU_SAND.Utils.ParseTemplates import parse_template

__author__ = 'Jasmine'

BUCKET = 'traxuscalc'


class BATRU_SANDConst:

    # P4 Template Update File:
    KPI_NAME_FIELD = 'KPI Display Name'
    GROUP_NAME_FIELD = 'Group Name'
    PRODUCT_NAME_FIELD = 'Atomic KPI Name'
    PRODUCT_SR_NAME_FIELD = 'Product Name'
    COLUMNS_FOR_STATIC = [KPI_NAME_FIELD, GROUP_NAME_FIELD, PRODUCT_NAME_FIELD]
    GROUP_NAME_P4 = 'Group Name'

    # static data
    SAS_SET_NAME = 'SAS'
    P4_SET_NAME = 'POSM Status'
    SK_SET_NAME = 'SK'
    P4_COUNT_FIXTURE = '{} # {}'
    P3_COUNT_FIXTURE = '{} - {}'
    MAX_KPI_COUNT = 10
    GROUP_RELATIVE_SCORE = 1
    PRODUCT_RELATIVE_SCORE = 0

    # sheets:
    SAS_ZONE_SHEET = 'SAS Zone Compliance'
    SK_SHEET = 'Sections'
    POSM_SHEET = 'Availability'

    # generic names for DB and dicts:
    SET_NAME = "kpi_set_name"
    KPI_NAME = "kpi_name"
    ATOMIC_NAME = "atomic_kpi_name"
    SET_FK = "kpi_set_fk"
    KPI_FK = "kpi_fk"
    ATOMIC_FK = "atomic_kpi_fk"

    # p3 Template columns and adress:
    WEIGHT = 'Weight'
    MODEL_ID = 'model_id'
    RELATIVE_SCORE = "relative_score"
    DISPLAY_TEXT = 'display_text'
    SKU_PRESENCE = 'SKU presence in SKU_List'
    SKU_SEQUENCE = 'SKU sequence'
    SKU_REPEATING = 'SKU repeating'
    convert_names = {
        MODEL_ID: 'model_id',
        SKU_PRESENCE: u'Наличие',
        SKU_SEQUENCE: u'Порядок по master plano',
        SKU_REPEATING: u'Корректность удвоений'
    }

    P4_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'p4_template.xlsx')
    P3_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'P3_template.xlsx')


class BATRU_SANDNewTemplate:

    def __init__(self, project_name, set_name):
        self.project = project_name
        self.log_suffix = '{}: '.format(self.project)
        self.queries = []
        self.kpi = set_name
        self.sets_added = {}
        self.kpis_added = {}
        self.kpi_counter = {'set': 0, 'kpi': 0, 'atomic': 0}
        self.data = pd.DataFrame()
        self.set_fk = self.get_set_fk(set_name)
        if set_name == BATRU_SANDConst.SK_SET_NAME:
            self.delete_static_DB()
            self.aws_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)
            self.kpi_static_data = self.get_kpi_data()
            self.get_kpis_from_template()
        elif set_name == BATRU_SANDConst.SAS_SET_NAME:
            self.aws_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)
            self.kpi_static_data = self.get_kpi_data()
            self.get_kpis_from_template_sas()
        elif set_name == BATRU_SANDConst.P4_SET_NAME:
            self.delete_static_DB()
            self.kpi_static_data = self.get_kpi_data()
            self.p4_template = parse_template(BATRU_SANDConst.P4_PATH, BATRU_SANDConst.POSM_SHEET)
            for column in self.p4_template.columns:
                self.p4_template[column] = self.encode_column_in_df(self.p4_template, column)
            self.alreadyAddedAtomics = pd.DataFrame(columns=[BATRU_SANDConst.SET_NAME, BATRU_SANDConst.KPI_NAME, BATRU_SANDConst.GROUP_NAME_P4,
                                                             BATRU_SANDConst.ATOMIC_NAME])

    def get_kpis_from_template_sas(self):
        list_of_dicts = []
        sections_template = parse_template(BATRU_SANDConst.P3_PATH, BATRU_SANDConst.SAS_ZONE_SHEET)
        fixtures = sections_template['Equipment'].unique()
        display_names = list(sections_template['display_name'].unique())
        display_names.append("No competitors in SAS Zone")
        for fixture in fixtures:
            for i in range(0, 11):
                if i == 0:
                    level_2_name = fixture
                else:
                    level_2_name = BATRU_SANDConst.P3_COUNT_FIXTURE.format(fixture, i)
                for level_3_name in display_names:
                    kpi_dictionary = {BATRU_SANDConst.SET_NAME: BATRU_SANDConst.SAS_SET_NAME,
                                      BATRU_SANDConst.KPI_NAME: level_2_name,
                                      BATRU_SANDConst.ATOMIC_NAME: level_3_name}
                    list_of_dicts.append(kpi_dictionary)
        self.data = pd.DataFrame(list_of_dicts)

    @staticmethod
    def encode_column_in_df(df, column_name):
        return df[column_name].str.encode('utf-8')

    def get_kpis_from_template(self):
        list_of_dicts = []
        sections_template = parse_template(BATRU_SANDConst.P3_PATH, BATRU_SANDConst.SK_SHEET)
        fixtures = sections_template['fixture'].unique()
        sections = sections_template['section_name'].unique()
        for fixture in fixtures:
            for i in range(0, 11):
                if i == 0:
                    level_2_name = fixture
                else:
                    level_2_name = fixture + " - {}".format(i)
                for model_id in sections:
                    for name in BATRU_SANDConst.convert_names.keys():
                        if name == BATRU_SANDConst.MODEL_ID:
                            level_3_name = model_id
                            display_text = model_id
                            relativ_score = 1
                        else:
                            level_3_name = name
                            relativ_score = 0
                            display_text = self.encode_string(BATRU_SANDConst.convert_names[name])
                        kpi_dictionary = {BATRU_SANDConst.SET_NAME: BATRU_SANDConst.SK_SET_NAME,
                                          BATRU_SANDConst.KPI_NAME: level_2_name,
                                          BATRU_SANDConst.ATOMIC_NAME: level_3_name,
                                          BATRU_SANDConst.MODEL_ID: model_id,
                                          BATRU_SANDConst.RELATIVE_SCORE: relativ_score,
                                          BATRU_SANDConst.DISPLAY_TEXT: display_text}
                        list_of_dicts.append(kpi_dictionary)
        self.data = pd.DataFrame(list_of_dicts)

    @staticmethod
    def encode_string(str):
        try:
            return str.replace("'", "\\'").encode('utf-8')
        except:
            Log.debug('The name {} is already coded'.format(str))
            return str

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)
        return self._rds_conn

    def delete_static_DB(self):
        cur = self.rds_conn.db.cursor()
        atomic_query = """
            delete from static.atomic_kpi
            where kpi_fk in (select pk from static.kpi where kpi_set_fk = {});
            """.format(self.set_fk)
        kpi_query = """
            delete from static.kpi where kpi_set_fk = {};
            """.format(self.set_fk)
        delete_queries = [atomic_query, kpi_query]
        for query in delete_queries:
            cur.execute(query)
            print query
        self.rds_conn.db.commit()

    def get_set_fk(self, set_name):
        self.rds_conn.connect_rds()
        query = """
                select pk
                from static.kpi_set where name = "{}";
                """.format(set_name)
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data.iloc[0][0]

    def get_kpi_data(self):
        self.rds_conn.connect_rds()
        query = """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk, api.description,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk, api.model_id as section,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.kpi_set kps
            left join static.kpi kpi on kps.pk = kpi.kpi_set_fk
            left join static.atomic_kpi api on kpi.pk = api.kpi_fk;
        """
        kpi_data = pd.read_sql_query(query, self.rds_conn.db)
        str_columns = ['description', 'kpi_name', 'atomic_kpi_name', 'kpi_set_name', 'section']
        for column in str_columns:
            kpi_data[column] = self.encode_column_in_df(kpi_data, column)
        return kpi_data

    def handle_update(self):
        if self.kpi == BATRU_SANDConst.P4_SET_NAME:
            self.add_p4_to_static()
            self.commit_to_db()
        elif self.kpi == BATRU_SANDConst.SK_SET_NAME:
            self.add_kpis_to_static_p3()
            self.add_atomics_to_static_p3()
            Log.info('{} Sets, {} KPIs and {} Atomics have been added'.format(self.kpi_counter['set'],
                                                                              self.kpi_counter['kpi'],
                                                                              self.kpi_counter['atomic']))
        elif self.kpi == BATRU_SANDConst.SAS_SET_NAME:
            self.add_kpis_to_static_sas()
            self.add_atomics_to_static_sas()
            Log.info('{} Sets, {} KPIs and {} Atomics have been added'.format(self.kpi_counter['set'],
                                                                              self.kpi_counter['kpi'],
                                                                              self.kpi_counter['atomic']))

    def add_kpis_to_static_sas(self):
        kpis = self.data.drop_duplicates(
            subset=[BATRU_SANDConst.SET_NAME, BATRU_SANDConst.KPI_NAME], keep='first')
        self.aws_conn.connect_rds()
        cur = self.aws_conn.db.cursor()
        for i in xrange(len(kpis)):
            set_name = self.encode_string(kpis.iloc[i][BATRU_SANDConst.SET_NAME])
            kpi_name = self.encode_string(kpis.iloc[i][BATRU_SANDConst.KPI_NAME])
            if self.kpi_static_data[(self.kpi_static_data[BATRU_SANDConst.SET_NAME] == set_name) &
                                    (self.kpi_static_data[BATRU_SANDConst.KPI_NAME] == kpi_name)].empty:
                set_fk = self.kpi_static_data[self.kpi_static_data[
                    BATRU_SANDConst.SET_NAME] == set_name][BATRU_SANDConst.SET_FK].values[0]
                level2_query = """
                       INSERT INTO static.kpi (kpi_set_fk, display_text)
                       VALUES ('{0}', '{1}');""".format(set_fk, kpi_name)
                print level2_query
                cur.execute(level2_query)
                if set_name in self.kpis_added.keys():
                    self.kpis_added[set_name][kpi_name] = cur.lastrowid
                else:
                    self.kpis_added[set_name] = {kpi_name: cur.lastrowid}
                print level2_query
                self.kpi_counter['kpi'] += 1
        self.aws_conn.db.commit()

    def add_atomics_to_static_sas(self):
        atomics = self.data
        queries = []
        for i in xrange(len(atomics)):
            atomic = atomics.iloc[i]
            set_name = self.encode_string(atomic[BATRU_SANDConst.SET_NAME])
            kpi_name = self.encode_string(atomic[BATRU_SANDConst.KPI_NAME])
            atomic_name = self.encode_string(atomic[BATRU_SANDConst.ATOMIC_NAME])
            names = [atomic_name]
            for index, name in enumerate(names):
                if self.kpi_static_data[(self.kpi_static_data[BATRU_SANDConst.SET_NAME] == set_name) &
                                        (self.kpi_static_data[BATRU_SANDConst.KPI_NAME] == kpi_name) &
                                        (self.kpi_static_data[BATRU_SANDConst.ATOMIC_NAME] == name)].empty:
                    if set_name in self.kpis_added.keys() and kpi_name in self.kpis_added[set_name].keys():
                        kpi_fk = self.kpis_added[set_name][kpi_name]
                    else:
                        kpi_fk = self.kpi_static_data[(self.kpi_static_data[BATRU_SANDConst.SET_NAME] == set_name) &
                                                      (self.kpi_static_data[
                                                          BATRU_SANDConst.KPI_NAME] == kpi_name)][BATRU_SANDConst.KPI_FK].values[0]
                    level3_query = """
                               INSERT INTO static.atomic_kpi (kpi_fk, name, description, display_text,
                                                              presentation_order, display)
                               VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(kpi_fk, name, name,
                                                                                            name, 1, 'Y')
                    queries.append(level3_query)
                    self.kpi_counter['atomic'] += 1
        self.aws_conn.connect_rds()
        cur = self.aws_conn.db.cursor()
        for query in queries:
            cur.execute(query)
            print query
        self.aws_conn.db.commit()

    def add_kpis_to_static_p3(self):
        kpis = self.data.drop_duplicates(
            subset=[BATRU_SANDConst.SET_NAME, BATRU_SANDConst.KPI_NAME], keep='first')
        self.aws_conn.connect_rds()
        cur = self.aws_conn.db.cursor()
        for i in xrange(len(kpis)):
            set_name = self.encode_string(kpis.iloc[i][BATRU_SANDConst.SET_NAME])
            kpi_name = self.encode_string(kpis.iloc[i][BATRU_SANDConst.KPI_NAME])
            if self.kpi_static_data[(self.kpi_static_data[BATRU_SANDConst.SET_NAME] == set_name) &
                                    (self.kpi_static_data[BATRU_SANDConst.KPI_NAME] == kpi_name)].empty:
                if set_name in self.sets_added.keys():
                    set_fk = self.sets_added[set_name]
                else:
                    set_fk = self.set_fk
                level2_query = """
                       INSERT INTO static.kpi (kpi_set_fk, display_text)
                       VALUES ('{0}', '{1}');""".format(set_fk, kpi_name)
                cur.execute(level2_query)
                if set_name in self.kpis_added.keys():
                    self.kpis_added[set_name][kpi_name] = cur.lastrowid
                else:
                    self.kpis_added[set_name] = {kpi_name: cur.lastrowid}
                print level2_query
                self.kpi_counter['kpi'] += 1
        self.aws_conn.db.commit()

    def add_atomics_to_static_p3(self):
        atomics = self.data
        queries = []
        for i in xrange(len(atomics)):
            atomic = atomics.iloc[i]
            set_name = self.encode_string(atomic[BATRU_SANDConst.SET_NAME])
            kpi_name = self.encode_string(atomic[BATRU_SANDConst.KPI_NAME])
            atomic_name = self.encode_string(atomic[BATRU_SANDConst.ATOMIC_NAME])
            model_id = self.encode_string(atomic[BATRU_SANDConst.MODEL_ID])
            relative_score = atomic[BATRU_SANDConst.RELATIVE_SCORE]
            display_text = self.encode_string(atomic[BATRU_SANDConst.DISPLAY_TEXT])
            names = [atomic_name]
            for index, name in enumerate(names):
                if self.kpi_static_data[(self.kpi_static_data[BATRU_SANDConst.SET_NAME] == set_name) &
                                        (self.kpi_static_data[BATRU_SANDConst.KPI_NAME] == kpi_name) &
                                        (self.kpi_static_data[BATRU_SANDConst.ATOMIC_NAME] == name) &
                                        (self.kpi_static_data['section'] == model_id)].empty:
                    if set_name in self.kpis_added.keys() and kpi_name in self.kpis_added[set_name].keys():
                        kpi_fk = self.kpis_added[set_name][kpi_name]
                    else:
                        kpi_fk = self.kpi_static_data[(self.kpi_static_data[BATRU_SANDConst.SET_NAME] == set_name) &
                                                      (self.kpi_static_data[BATRU_SANDConst.KPI_NAME] == kpi_name)][
                            BATRU_SANDConst.KPI_FK].values[0]
                    level3_query = """
                               INSERT INTO static.atomic_kpi (kpi_fk, name, description, display_text,
                                                              presentation_order, model_id, relative_score, display)
                               VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}');
                               """.format(kpi_fk, name, name, display_text, index + 1, model_id, relative_score, 'Y')
                    queries.append(level3_query)
                    self.kpi_counter['atomic'] += 1
        self.aws_conn.connect_rds()
        cur = self.aws_conn.db.cursor()
        for query in queries:
            cur.execute(query)
            print query
        self.aws_conn.db.commit()

    def add_p4_to_static(self):
        template_for_static = self.p4_template[BATRU_SANDConst.COLUMNS_FOR_STATIC]
        # Saves to static all KPI (equipments) with a counter.
        atomic_queries = []
        kpi_queries = []
        kpi_names = template_for_static[BATRU_SANDConst.KPI_NAME_FIELD].unique().tolist()
        for kpi in kpi_names:
            kpi_with_count = self.add_kpi_count(kpi)
            for kpi_count in kpi_with_count:
                if self.kpi_static_data[
                    (self.kpi_static_data['kpi_set_name'] == BATRU_SANDConst.P4_SET_NAME) &
                        (self.kpi_static_data['kpi_name'] == kpi_count)].empty:
                    kpi_queries.append(kpi_count)
        self.save_kpi_level(self.set_fk, kpi_queries)
        # We need to re-run query for updated kpis.
        self.kpi_static_data = self.get_kpi_data()
        # This part is not combined with the loop above since it needs all kpis (with count) to be saved first.
        for kpi_name in kpi_names:
            atomics_for_static = template_for_static[template_for_static[BATRU_SANDConst.KPI_NAME_FIELD] == kpi_name]
            for i in xrange(len(atomics_for_static)):
                row = atomics_for_static.iloc[i]
                group = self.encode_string(row[BATRU_SANDConst.GROUP_NAME_FIELD])
                product = self.encode_string(row[BATRU_SANDConst.PRODUCT_NAME_FIELD])
                kpi_with_count = self.add_kpi_count(kpi_name)
                #  This will create group and product atomics
                for kpi in kpi_with_count:
                    is_exist = self.alreadyAddedAtomics[(self.alreadyAddedAtomics[
                        BATRU_SANDConst.SET_NAME] == BATRU_SANDConst.P4_SET_NAME) &
                        (self.alreadyAddedAtomics[BATRU_SANDConst.KPI_NAME] == kpi) &
                        (self.alreadyAddedAtomics[BATRU_SANDConst.GROUP_NAME_P4] == group) &
                        (self.alreadyAddedAtomics[BATRU_SANDConst.ATOMIC_NAME] == product)]
                    if is_exist.empty:
                        try:

                            kpi_fk = self.kpi_static_data[(self.kpi_static_data[BATRU_SANDConst.SET_FK] == self.set_fk) &
                                                          (self.kpi_static_data[BATRU_SANDConst.KPI_NAME] == kpi)][
                                BATRU_SANDConst.KPI_FK].values[0]
                            dict_already_added = {BATRU_SANDConst.SET_NAME: BATRU_SANDConst.P4_SET_NAME, BATRU_SANDConst.KPI_NAME: kpi,
                                                  BATRU_SANDConst.GROUP_NAME_P4: group, BATRU_SANDConst.ATOMIC_NAME: product}
                            self.alreadyAddedAtomics = self.alreadyAddedAtomics.append(
                                dict_already_added, ignore_index=True)
                            product_query = (kpi_fk, product, product, product,
                                             group, BATRU_SANDConst.PRODUCT_RELATIVE_SCORE)
                            group_query = (kpi_fk, group, group, group, None,
                                           BATRU_SANDConst.GROUP_RELATIVE_SCORE)
                            atomic_queries.extend([group_query, product_query])
                        except IndexError as e:
                            print "kpi '{}' does not exist.".format(kpi)
        self.create_atomic_queries(set(atomic_queries))

    def create_atomic_queries(self, queries_to_commit):
        level3_query = """
        INSERT INTO static.atomic_kpi (kpi_fk, name, description, display_text,
                                       presentation_order, display, model_id, relative_score)
        VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', {});"""
        for query in queries_to_commit:
            if self.is_new(query):
                self.queries.append(level3_query.format(query[0], query[1], query[2], query[3],
                                                        1, 'Y', '{}'.format(
                                                            query[4]) if query[4] else 'NULL', query[5]
                                                        ).replace("'NULL'", "NULL"))

    def is_new(self, data, level=3):
        if level == 3:
            existing = self.kpi_static_data[(self.kpi_static_data[BATRU_SANDConst.SET_FK] == self.set_fk) &
                                            (self.kpi_static_data[BATRU_SANDConst.KPI_FK] == data[0]) &
                                            (self.kpi_static_data[BATRU_SANDConst.ATOMIC_NAME] == data[1])]
        elif level == 2:
            existing = self.kpi_static_data[(self.kpi_static_data[BATRU_SANDConst.SET_FK] == self.set_fk) &
                                            (self.kpi_static_data[BATRU_SANDConst.KPI_NAME] == data[0])]
        else:
            Log.debug('not valid level for checking new KPIs')
            return False

        return existing.empty

    def save_kpi_level(self, set_fk, kpi_list):
        level2_query = """
            INSERT INTO static.kpi (kpi_set_fk, display_text)
            VALUES ('{}', '{}');"""
        new_kpis = []
        for kpi in kpi_list:
            if self.is_new([kpi], level=2):
                new_kpis.append(kpi)
        count_for_show = 0
        self.rds_conn.connect_rds()
        all = len(kpi_list)
        cur = self.rds_conn.db.cursor()
        for kpi in new_kpis:
            query = level2_query.format(set_fk, kpi.replace("'", "''"))
            print query
            count_for_show += 1
            cur.execute(query)
            if count_for_show % 10 == 0:
                print 'done {} / {}'.format(count_for_show, all)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()

    def add_kpi_count(self, kpi_name):
        kpis = [kpi_name]
        i = 2
        while i <= BATRU_SANDConst.MAX_KPI_COUNT:
            kpi = BATRU_SANDConst.P4_COUNT_FIXTURE.format(kpi_name, i)
            kpis.append(kpi)
            i += 1
        return kpis

    def commit_to_db(self):
        self.rds_conn.connect_rds()
        cur = self.rds_conn.db.cursor()
        kpis_sum = len(self.queries)
        count_for_show = 0
        for query in self.queries:
            # try:
            print query
            cur.execute(query)
            count_for_show += 1
            if count_for_show % 10 == 0:
                print 'There are {} / {}'.format(count_for_show, kpis_sum)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()


if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('New BATRU Template')
    project_name = 'batru_sand'
    kpi_names = [
        # FOR P1: there is a black line "self.tools.upload_store_assortment_file(P1_PATH)". We only need to paste
        # the template in Data/StoreAssortment.csv, activate this line and run it.
        BATRU_SANDConst.P4_SET_NAME,
        # BATRU_SANDConst.SK_SET_NAME,
        # BATRU_SANDConst.SAS_SET_NAME,
    ]
    for kpi_name in kpi_names:
        template = BATRU_SANDNewTemplate(project_name, kpi_name)
        template.handle_update()
