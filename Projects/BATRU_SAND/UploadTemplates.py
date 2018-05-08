
import os
import pandas as pd
import getpass
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Conf.Configuration import Config
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log
from Projects.BATRU_SAND.Utils.ParseTemplates import parse_template

__author__ = 'Jasmine'

BUCKET = 'traxuscalc'


class Const:


    # P4 Template Update File:
    SET_NAME_FIELD = 'L0 - SET'
    KPI_NAME_FIELD = 'L1 - Equipment (scene type attribute 1)'
    GROUP_NAME_FIELD = 'L2 - V3 Name'
    PRODUCT_NAME_FIELD = 'L3 - Display Name POSM'
    PRODUCT_SR_NAME_FIELD = 'Names of template in SR'
    STORE_TYPE_FIELD = 'Filter stores by \'attribute 3\''
    TEMPLATE_GROUP_FIELD = 'Filter scenes by \'template group\''
    COLUMNS_FOR_STATIC = [SET_NAME_FIELD, KPI_NAME_FIELD, GROUP_NAME_FIELD, PRODUCT_NAME_FIELD]

    # static data
    P4_SET_NAME = 'POSM Status'
    SK_SET_NAME = 'SK'
    SAS = 'SAS'
    KPI_COUNT_NAME = '{} # {}'
    MAX_KPI_COUNT = 10
    GROUP_RELATIVE_SCORE = 1
    PRODUCT_RELATIVE_SCORE = 0

    # P4 Template used in KPI:
    GROUP_NAME_P4 = 'Group Name'
    PRODUCT_SR_NAME_P4 = 'Product Name'
    STORE_TYPE_P4 = 'Filter stores by \'attribute 3\''
    TEMPLATE_GROUP_P4 = 'Template Group'

    # kpi update
    P4_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'p4_template.xlsx')
    TMP_CACHE_SRC_FOLDER = '/home/{}/Desktop/'.format(getpass.getuser())
    TMP_CATCH = os.path.join(TMP_CACHE_SRC_FOLDER, 'batru_old_p4_template.xlsx')


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
    sk_names = [MODEL_ID,
                SKU_PRESENCE,
                SKU_SEQUENCE,
                SKU_REPEATING]
    P3_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'P3_template.xlsx')
    P3_CONVERT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'convert names.xlsx')


class BATRU_SANDNewTemplate:

    def __init__(self, set_name, path=None):
        self.project = 'batru-sand'
        self.path = path
        self.log_suffix = '{}: '.format(self.project)
        self.queries = []
        self.kpi = set_name
        self.sets_added = {}
        self.kpis_added = {}
        self.kpi_counter = {'set': 0, 'kpi': 0, 'atomic': 0}
        self.data = pd.DataFrame()
        self.set_fk = self.get_set_fk(set_name)
        if set_name == Const.SK_SET_NAME:
            self.delete_static_DB()
            self.aws_conn = AwsProjectConnector(self.project, DbUsers.CalculationEng)
            self.kpi_static_data = self.get_kpi_static_data_SK()
            self.get_kpis_from_template()
        elif set_name == Const.SAS:
            self.delete_static_DB()
            self.aws_conn = AwsProjectConnector(self.project, DbUsers.CalculationEng)
            self.kpi_static_data = self.get_kpi_data_sas()
            self.get_kpis_from_template_sas()
        elif set_name == Const.P4_SET_NAME:
            self.delete_static_DB()
            self.kpi_static_data = self.get_kpi_data_sas()
            self.p4_template = pd.read_excel(self.path)
            self.alreadyAddedAtomics = pd.DataFrame(columns=[Const.SET_NAME, Const.KPI_NAME, Const.GROUP_NAME_P4,
                                                             Const.ATOMIC_NAME])

    def get_kpis_from_template_sas(self):
        list_of_dicts = []
        sections_template = parse_template(Const.P3_PATH, 'SAS Zone Compliance')
        fixtures = sections_template['Equipment'].unique()
        display_names = list(sections_template['display_name'].unique())
        display_names.append("No competitors in SAS Zone")
        for fixture in fixtures:
            for i in range(0, 11):
                if i == 0:
                    level_2_name = fixture
                else:
                    level_2_name = fixture + " - {}".format(i)
                for level_3_name in display_names:
                    kpi_dictionary = {Const.SET_NAME: "SAS",
                                      Const.KPI_NAME: level_2_name,
                                      Const.ATOMIC_NAME: level_3_name}
                    list_of_dicts.append(kpi_dictionary)
        self.data = pd.DataFrame(list_of_dicts)

    def get_kpis_from_template(self):
        list_of_dicts = []
        convert_names = parse_template(Const.P3_CONVERT_PATH, 'KPIs')
        sections_template = parse_template(Const.P3_PATH, 'Sections')
        fixtures = sections_template['fixture'].unique()
        sections = sections_template['section_name'].unique()
        for fixture in fixtures:
            for i in range(0, 11):
                if i == 0:
                    level_2_name = fixture
                else:
                    level_2_name = fixture + " - {}".format(i)
                for model_id in sections:
                    for name in Const.sk_names:
                        if name == Const.MODEL_ID:
                            level_3_name = model_id
                            display_text = model_id
                            relativ_score = Const.GROUP_RELATIVE_SCORE
                        else:
                            level_3_name = name
                            relativ_score = Const.PRODUCT_RELATIVE_SCORE
                            display_text = convert_names[
                                convert_names['name'] == name]['display_text'].iloc[0]
                        kpi_dictionary = {Const.SET_NAME: Const.SK_SET_NAME,
                                          Const.KPI_NAME: level_2_name,
                                          Const.ATOMIC_NAME: level_3_name,
                                          Const.MODEL_ID: model_id,
                                          Const.RELATIVE_SCORE: relativ_score,
                                          Const.DISPLAY_TEXT: display_text}
                        list_of_dicts.append(kpi_dictionary)
        self.data = pd.DataFrame(list_of_dicts)

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = AwsProjectConnector(self.project, DbUsers.CalculationEng)
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

    def get_kpi_static_data_SK(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = """
                select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                       kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                       kps.name as kpi_set_name, kps.pk as kpi_set_fk,
                       api.model_id as section
                from static.atomic_kpi api
                left join static.kpi kpi on kpi.pk = api.kpi_fk
                join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
                """
        kpi_static_data = pd.read_sql_query(query, self.aws_conn.db)
        return kpi_static_data

    def get_kpi_data_sas(self):
        self.rds_conn.connect_rds()
        query = """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk, api.description,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.kpi_set kps
            left join static.kpi kpi on kps.pk = kpi.kpi_set_fk
            left join static.atomic_kpi api on kpi.pk = api.kpi_fk;
        """
        kpi_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_data

    def handle_update(self):
        if self.kpi == Const.P4_SET_NAME:
            self.add_p4_to_static()
            self.commit_to_db()
            self.update_templates_in_folder()
        elif self.kpi == Const.SK_SET_NAME:
            self.add_kpis_to_static_SK()
            self.add_atomics_to_static_SK()
            Log.info('{} Sets, {} KPIs and {} Atomics have been added'.format(self.kpi_counter['set'],
                                                                              self.kpi_counter['kpi'],
                                                                              self.kpi_counter['atomic']))
        elif self.kpi == Const.SAS:
            self.add_kpis_to_static_sas()
            self.add_atomics_to_static_sas()
            Log.info('{} Sets, {} KPIs and {} Atomics have been added'.format(self.kpi_counter['set'],
                                                                              self.kpi_counter['kpi'],
                                                                              self.kpi_counter['atomic']))

    def add_kpis_to_static_sas(self):
        kpis = self.data.drop_duplicates(subset=[Const.SET_NAME, Const.KPI_NAME], keep='first')
        self.aws_conn.connect_rds()
        cur = self.aws_conn.db.cursor()
        for i in xrange(len(kpis)):
            set_name = kpis.iloc[i][Const.SET_NAME].replace("'", "\\'").encode('utf-8')
            kpi_name = kpis.iloc[i][Const.KPI_NAME].replace("'", "\\'").encode('utf-8')
            if self.kpi_static_data[(self.kpi_static_data[Const.SET_NAME] == set_name) & (
                        self.kpi_static_data[Const.KPI_NAME] == kpi_name)].empty:
                set_fk = self.kpi_static_data[self.kpi_static_data[
                                                  Const.SET_NAME] == set_name][Const.SET_FK].values[0]
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
            set_name = atomic[Const.SET_NAME].replace("'", "\\'").encode('utf-8')
            kpi_name = atomic[Const.KPI_NAME].replace("'", "\\'").encode('utf-8')
            atomic_name = atomic[Const.ATOMIC_NAME].replace("'", "\\'").encode('utf-8')
            names = [atomic_name]
            for index, name in enumerate(names):
                if self.kpi_static_data[(self.kpi_static_data[Const.SET_NAME] == set_name) &
                        (self.kpi_static_data[Const.KPI_NAME] == kpi_name) &
                        (self.kpi_static_data[Const.ATOMIC_NAME] == name)].empty:
                    if set_name in self.kpis_added.keys() and kpi_name in self.kpis_added[set_name].keys():
                        kpi_fk = self.kpis_added[set_name][kpi_name]
                    else:
                        kpi_fk = self.kpi_static_data[(self.kpi_static_data[Const.SET_NAME] == set_name) &
                                                      (self.kpi_static_data[
                                                           Const.KPI_NAME] == kpi_name)][Const.KPI_FK].values[0]
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

    def add_kpis_to_static_SK(self):
        kpis = self.data.drop_duplicates(subset=[Const.SET_NAME, Const.KPI_NAME], keep='first')
        self.aws_conn.connect_rds()
        cur = self.aws_conn.db.cursor()
        for i in xrange(len(kpis)):
            set_name = kpis.iloc[i][Const.SET_NAME].replace("'", "\\'").encode('utf-8')
            kpi_name = kpis.iloc[i][Const.KPI_NAME].replace("'", "\\'").encode('utf-8')
            if self.kpi_static_data[(self.kpi_static_data[Const.SET_NAME] == set_name) &
                    (self.kpi_static_data[Const.KPI_NAME] == kpi_name)].empty:
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

    def add_atomics_to_static_SK(self):
        atomics = self.data
        queries = []
        for i in xrange(len(atomics)):
            atomic = atomics.iloc[i]
            set_name = atomic[Const.SET_NAME].replace("'", "\\'").encode('utf-8')
            kpi_name = atomic[Const.KPI_NAME].replace("'", "\\'").encode('utf-8')
            atomic_name = atomic[Const.ATOMIC_NAME].replace("'", "\\'").encode('utf-8')
            model_id = atomic[Const.MODEL_ID].replace("'", "\\'").encode('utf-8')
            relative_score = atomic[Const.RELATIVE_SCORE]
            display_text = atomic[Const.DISPLAY_TEXT].replace("'", "\\'").encode('utf-8')
            names = [atomic_name]
            for index, name in enumerate(names):
                if self.kpi_static_data[(self.kpi_static_data[Const.SET_NAME] == set_name) &
                        (self.kpi_static_data[Const.KPI_NAME] == kpi_name) &
                        (self.kpi_static_data[Const.ATOMIC_NAME] == name) &
                        (self.kpi_static_data['section'] == model_id)].empty:
                    if set_name in self.kpis_added.keys() and kpi_name in self.kpis_added[set_name].keys():
                        kpi_fk = self.kpis_added[set_name][kpi_name]
                    else:
                        kpi_fk = self.kpi_static_data[(self.kpi_static_data[Const.SET_NAME] == set_name) &
                                                      (self.kpi_static_data[Const.KPI_NAME] == kpi_name)][
                            Const.KPI_FK].values[0]
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
        template_for_static = self.p4_template[Const.COLUMNS_FOR_STATIC]
        invalid = template_for_static[template_for_static[Const.SET_NAME_FIELD] != Const.P4_SET_NAME]
        if not invalid.empty:
            Log.warning('Set names: \'{}\' was not found'.format((invalid[Const.SET_NAME_FIELD].unique())))
        template_for_static = template_for_static[template_for_static[Const.SET_NAME_FIELD] == Const.P4_SET_NAME]
        # Saves to static all KPI (equipments) with a counter.
        atomic_queries = []
        kpi_queries = []
        kpi_names = template_for_static[Const.KPI_NAME_FIELD].unique().tolist()
        for kpi in kpi_names:
            kpi_with_count = self.add_kpi_count(kpi)
            for kpi_count in kpi_with_count:
                if self.kpi_static_data[
                            (self.kpi_static_data['kpi_set_name'] == Const.P4_SET_NAME) &
                            (self.kpi_static_data['kpi_name'].str.encode('utf-8') == kpi_count)].empty:
                    kpi_queries.append(kpi_count)
        self.save_kpi_level(self.set_fk, kpi_queries)
        # We need to re-run query for updated kpis.
        self.kpi_static_data = self.get_kpi_data_sas()
        # This part is not combined with the loop above since it needs all kpis (with count) to be saved first.
        for kpi_name in kpi_names:
            atomics_for_static = template_for_static[template_for_static[Const.KPI_NAME_FIELD] == kpi_name]
            for i in xrange(len(atomics_for_static)):
                row = atomics_for_static.iloc[i]
                group = row[Const.GROUP_NAME_FIELD].replace("'", "''").encode('utf-8')
                product = row[Const.PRODUCT_NAME_FIELD].replace("'", "''").encode('utf-8')
                kpi_with_count = self.add_kpi_count(kpi_name)
                #  This will create group and product atomics
                for kpi in kpi_with_count:
                    is_exist = self.alreadyAddedAtomics[(self.alreadyAddedAtomics[
                                                             Const.SET_NAME] == Const.P4_SET_NAME) &
                                                        ((self.alreadyAddedAtomics[Const.KPI_NAME] == kpi) |
                                                         (self.alreadyAddedAtomics[
                                                             Const.KPI_NAME] == kpi.decode('utf8'))) &
                                                        (self.alreadyAddedAtomics[Const.GROUP_NAME_P4] == group) &
                                                        (self.alreadyAddedAtomics[Const.ATOMIC_NAME] == product)]
                    if is_exist.empty:
                        try:
                            kpi_fk = self.kpi_static_data[(self.kpi_static_data[Const.SET_FK] == self.set_fk) &
                                                          ((self.kpi_static_data[Const.KPI_NAME] == kpi) |
                                                           (self.kpi_static_data[Const.KPI_NAME] == kpi.decode(
                                                               'utf8')))][Const.KPI_FK].values[0]
                            dict_already_added = {Const.SET_NAME: Const.P4_SET_NAME, Const.KPI_NAME: kpi,
                                                  Const.GROUP_NAME_P4: group, Const.ATOMIC_NAME: product}
                            self.alreadyAddedAtomics = self.alreadyAddedAtomics.append(
                                dict_already_added, ignore_index=True)
                            product_query = (kpi_fk, product, product, product, group, Const.PRODUCT_RELATIVE_SCORE)
                            group_query = (kpi_fk, group, group, group, None, Const.GROUP_RELATIVE_SCORE)
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
                                                        1, 'Y', '{}'.format(query[4]) if query[4] else 'NULL', query[5])
                                    ).replace("'NULL'", "NULL")

    def is_new(self, data, level=3):
        if level == 3:
            existing = self.kpi_static_data[(self.kpi_static_data[Const.SET_FK] == self.set_fk) &
                                            (self.kpi_static_data[Const.KPI_FK] == data[0]) &
                                            ((self.kpi_static_data[Const.ATOMIC_NAME] == data[1]) |
                                             (self.kpi_static_data[Const.ATOMIC_NAME] == data[1].decode('utf-8')))]
        elif level == 2:
            existing = self.kpi_static_data[(self.kpi_static_data[Const.SET_FK] == self.set_fk) &
                                            ((self.kpi_static_data[Const.KPI_NAME] == data[0].decode('utf-8')) |
                                             (self.kpi_static_data[Const.KPI_NAME] == data[0]))]
        else:
            Log.info('not valid level for checking new KPIs')
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
        kpi_name = kpi_name.encode('utf-8')
        kpis = [kpi_name]
        i = 2
        while i <= Const.MAX_KPI_COUNT:
            kpi = Const.KPI_COUNT_NAME.format(kpi_name, i)
            kpis.append(kpi)
            i += 1
        return kpis

    def p4_format_new_template(self):
        self.p4_template = self.p4_template.rename(columns={Const.KPI_NAME_FIELD: Const.KPI_NAME,
                                                            Const.PRODUCT_NAME_FIELD: Const.ATOMIC_NAME,
                                                            Const.PRODUCT_SR_NAME_FIELD: Const.PRODUCT_SR_NAME_P4,
                                                            Const.TEMPLATE_GROUP_FIELD: Const.TEMPLATE_GROUP_P4,
                                                            Const.STORE_TYPE_FIELD: Const.STORE_TYPE_P4,
                                                            Const.GROUP_NAME_FIELD: Const.GROUP_NAME_P4})

        self.p4_template = self.p4_template[[Const.KPI_NAME, Const.GROUP_NAME_P4, Const.ATOMIC_NAME,
                                             Const.PRODUCT_SR_NAME_P4, Const.STORE_TYPE_P4, Const.TEMPLATE_GROUP_P4]]

    def update_templates_in_folder(self):
        """
        This will replace the current file to the new template.
        :return:
        """
        self.p4_format_new_template()
        if os.path.exists(Const.P4_PATH):
            os.rename(Const.P4_PATH, Const.TMP_CATCH)
        self.p4_template.to_excel(Const.P4_PATH)

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
            # except Err as e:
            #     print
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()

# if __name__ == '__main__':
#     Config.init()
#     Log.init('New Batru Template')
#     template_path = '/home/Elyashiv/Downloads/POSM Matrix C_04_final_17.04.xlsx'
#     kpi_name = Const.P4_SET_NAME
#     template = BATRU_SANDNewTemplate(kpi_name, template_path)
#     template.handle_update()
