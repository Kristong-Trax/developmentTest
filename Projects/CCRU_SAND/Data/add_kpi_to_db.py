
import pandas as pd

from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Cloud.Services.Connector.Keys import DbUsers
from mock import patch

__author__ = 'Nimrod'


class CCRU_SANDCustomConfigurations(object):

    CUSTOM_FIELD = 'KPI Type'
    DEFAULT_SUFFIXES = ['_{}'.format(i) for i in xrange(1, 11)]
    CUSTOM_DATA = {
        'Availability': {'number_of_atomics': 2, 'suffixes': ['-Availability', '-Share of Shelf']}
    }


class CCRU_SANDConsts(object):

    KPI_SET_NAME = 'KPI Level 1 Name'
    KPI_NAME = 'KPI Level 2 Name'
    ATOMIC_KPI_NAME = 'KPI Level 3 Name'
    WEIGHT = 'KPI Weight'
    MODEL_ID = 'model_id'
    LEVEL = 'level'
    SORTING = 'KPI ID'
    ENG_NAME = 'KPI name Eng'
    OPERATOR = 'Logical Operator'


class CCRU_SANDAddKPIs(CCRU_SANDConsts, CCRU_SANDCustomConfigurations):
    """
    This module writes all levels of KPIs to the DB, given a template.

    - The template file must include a unique row for every Atomic KPI (and ONLY Atomics)
    - Each row much include a set-name and a KPI-name columns (configured in 'CCRU_SANDConsts')
    - For every level of KPI (set, KPI or atomic) - only the ones who do not already exist would be added to the DB

    (Example for template: GILLETTEUS/Data/Template.xlsx)
    """

    def __init__(self, set_name, template_path, custom_mode=False):
        self.project = 'ccru_sand'
        self.aws_conn = AwsProjectConnector(self.project, DbUsers.CalculationEng)
        self.kpi_static_data = self.get_kpi_static_data()
        self.data = pd.read_excel(template_path)
        self.custom_mode = custom_mode
        self.set_name = set_name
        self.sets_added = {}
        self.kpis_added = {}
        self.kpi_counter = {'set': 0, 'kpi': 0, 'atomic': 0}

    def add_fields_to_exist_kpis(self):
        kpis, atomics = [], []
        for i in xrange(len(self.data)):
            params = self.data.iloc[i]
            fields_to_add = {"weight": None,
                             "presentation_order": None,
                             "logical_operator": None}
            name = params[CCRU_SANDConsts.ENG_NAME]
            level = params[CCRU_SANDConsts.LEVEL]
            if CCRU_SANDConsts.WEIGHT in params.keys():
                weight = params[CCRU_SANDConsts.WEIGHT]
                fields_to_add['weight'] = weight
            if CCRU_SANDConsts.OPERATOR in params.keys():
                fields_to_add['logical_operator'] = params[CCRU_SANDConsts.OPERATOR]
            if CCRU_SANDConsts.SORTING in params.keys():
                fields_to_add['presentation_order'] = params[CCRU_SANDConsts.SORTING]
            if level == 2 or level == "2":
                fields_to_add['display_text'] = name.replace("'", "\\'").encode('utf-8')
                kpis.append(fields_to_add)
            elif level == 3 or level == 4:
                fields_to_add['name'] = name.replace("'", "\\'").encode('utf-8')
                atomics.append(fields_to_add)
            else:
                continue

        cur = self.aws_conn.db.cursor()
        for kpi in kpis:
            level2_query = """
                UPDATE static.kpi k SET k.weight={0}, k.presentation_order = {1}
                 WHERE k.display_text="{2}" and k.kpi_set_fk in (select pk from static.kpi_set where name =
                 "{3}");""".format(kpi['weight'], kpi['presentation_order'],
                                   kpi['display_text'], self.set_name)
            print level2_query
            cur.execute(level2_query)
        for atomic in atomics:
            level2_query = """
                UPDATE static.atomic_kpi a SET a.atomic_weight = {0}, a.presentation_order = {1}
                 WHERE a.name="{2}" and kpi_fk in (select k.pk from static.kpi k join static.kpi_set s on
                  k.kpi_set_fk = s.pk where s.name = "{3}");""".format(atomic['weight'], atomic['presentation_order'],
                                                                       atomic['name'], self.set_name)
            print level2_query
            cur.execute(level2_query)
        self.aws_conn.db.commit()

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = """
                select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                       kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                       kps.name as kpi_set_name, kps.pk as kpi_set_fk
                from static.atomic_kpi api
                left join static.kpi kpi on kpi.pk = api.kpi_fk
                join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
                """
        kpi_static_data = pd.read_sql_query(query, self.aws_conn.db)
        return kpi_static_data

    def add_weights(self):
        """
        This function writes KPIs' weights to the DB, given a weight-column and a unique row for each KPI.
        """
        update_query = "update static.kpi set weight = '{}' where pk = {}"
        queries = []
        for kpi in self.data:
            kpi_fk = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == kpi.get(self.KPI_SET_NAME)) &
                                          (self.kpi_static_data['kpi_name'] == kpi.get(self.KPI_NAME))]['kpi_fk'].values[0]
            queries.append(update_query.format(kpi.get(self.WEIGHT), kpi_fk))
        if queries:
            cur = self.aws_conn.db.cursor()
            for query in queries:
                cur.execute(query)
        self.aws_conn.db.commit()

    def add_kpis_from_template(self):
        self.add_sets_to_static()
        self.add_kpis_to_static()
        self.add_atomics_to_static()
        Log.info('{} Sets, {} KPIs and {} Atomics have been added'.format(self.kpi_counter['set'],
                                                                          self.kpi_counter['kpi'],
                                                                          self.kpi_counter['atomic']))

    def add_sets_to_static(self):
        set_names = self.data[self.KPI_SET_NAME].unique().tolist()
        existing_set_names = self.kpi_static_data['kpi_set_name'].unique().tolist()
        set_names_to_add = set(set_names).difference(existing_set_names)
        if set_names_to_add:
            cur = self.aws_conn.db.cursor()
            for set_name in set_names_to_add:
                level1_query = """
                               INSERT INTO static.kpi_set (name, missing_kpi_score, enable, normalize_weight,
                                                           expose_to_api, is_in_weekly_report)
                               VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(set_name, 'Bad', 'Y',
                                                                                            'N', 'N', 'N')
                cur.execute(level1_query)
                self.sets_added[set_name] = cur.lastrowid
                self.kpi_counter['set'] += 1
            self.aws_conn.db.commit()

    def add_kpis_to_static(self):
        kpis = self.data.drop_duplicates(subset=[self.KPI_SET_NAME, self.KPI_NAME], keep='first')
        cur = self.aws_conn.db.cursor()
        for i in xrange(len(kpis)):
            set_name = kpis.iloc[i][self.KPI_SET_NAME].replace("'", "\\'").encode('utf-8')
            kpi_name = kpis.iloc[i][self.KPI_NAME].replace("'", "\\'").encode('utf-8')
            if self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                    (self.kpi_static_data['kpi_name'] == kpi_name)].empty:
                if set_name in self.sets_added.keys():
                    set_fk = self.sets_added[set_name]
                else:
                    set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name']
                                                  == set_name]['kpi_set_fk'].values[0]
                level2_query = """
                       INSERT INTO static.kpi (kpi_set_fk, display_text)
                       VALUES ('{0}', '{1}');""".format(set_fk, kpi_name)
                cur.execute(level2_query)
                if set_name in self.kpis_added.keys():
                    self.kpis_added[set_name][kpi_name] = cur.lastrowid
                else:
                    self.kpis_added[set_name] = {kpi_name: cur.lastrowid}
                self.kpi_counter['kpi'] += 1
        self.aws_conn.db.commit()

    def add_atomics_to_static(self):
        atomics = self.data.drop_duplicates(
            subset=[self.KPI_SET_NAME, self.KPI_NAME, self.ATOMIC_KPI_NAME], keep='first')
        cur = self.aws_conn.db.cursor()
        for i in xrange(len(atomics)):
            atomic = atomics.iloc[i]
            set_name = atomic[self.KPI_SET_NAME].replace("'", "\\'").encode('utf-8')
            kpi_name = atomic[self.KPI_NAME].replace("'", "\\'").encode('utf-8')
            atomic_name = atomic[self.ATOMIC_KPI_NAME].replace("'", "\\'").encode('utf-8')

            if self.custom_mode:
                names = []
                custom_indicator = atomic.get(self.CUSTOM_FIELD)
                if custom_indicator not in self.CUSTOM_DATA.keys():
                    Log.warning(
                        "Atomic KPI '{}' is not configured in the CUSTOM_DATA dictionary".format(atomic_name))
                    custom_data = {'number_of_atomics': 1, 'suffixes': ['']}
                else:
                    custom_data = self.CUSTOM_DATA.get(custom_indicator)
                number_of_atomics = custom_data.get('number_of_atomics', 1)
                suffixes = custom_data.get('suffixes')
                if not suffixes or len(suffixes) < number_of_atomics:
                    suffixes = self.DEFAULT_SUFFIXES
                for x in xrange(number_of_atomics):
                    names.append(atomic_name + suffixes[x])
            else:
                names = [atomic_name]

            for index, name in enumerate(names):
                if self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                        (self.kpi_static_data['kpi_name'] == kpi_name) &
                                        (self.kpi_static_data['atomic_kpi_name'] == name)].empty:
                    if set_name in self.kpis_added.keys() and kpi_name in self.kpis_added[set_name].keys():
                        kpi_fk = self.kpis_added[set_name][kpi_name]
                    else:
                        kpi_fk = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                                      (self.kpi_static_data['kpi_name'] == kpi_name)]['kpi_fk'].values[0]

                    level3_query = """
                               INSERT INTO static.atomic_kpi (kpi_fk, name, description, display_text,
                                                              presentation_order, display)
                               VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(kpi_fk, name, name, name,
                                                                                            index + 1, 'Y')
                    cur.execute(level3_query)
                    self.kpi_counter['atomic'] += 1
        self.aws_conn.db.commit()


# if __name__ == '__main__':
#     LoggerInitializer.init('test')
#     Config.init()
#     # kpis = {'Pos 2018 - FT': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/FT PoS 2018.xlsx',
#     #         'Pos 2018 - MT - Convenience Big': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/Convenience Big PoS 2018.xlsx',
#     #         'Pos 2018 - MT - Convenience Small': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/Convenience Small PoS 2018.xlsx',
#     #         'Pos 2018 - Canteen': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/Canteen PoS 2018.xlsx',
#     #         'Pos 2018 - Petrol': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/Petroleum PoS 2018.xlsx',
#     #         'Pos 2018 - MT - Hypermarket': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/Hypermarket PoS 2018.xlsx',
#     #         'Pos 2018 - HoReCa (Restaurant/Cafe)': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/HoReCa Restaurant_Cafe PoS 2018.xlsx',
#     #         'Pos 2018 - HoReCa (Cofee /Tea Shops)': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/HoReCa Cofee_Tea Shops PoS 2018.xlsx',
#     #         'Pos 2018 - HoReCa (Bar Tavern/Night Clubs)': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/HoReCa Bar Tavern_Night Club PoS 2018.xlsx',
#     #         'Pos 2018 - QSR': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/QSR PoS 2018.xlsx',
#     #         'Pos 2018 - MT - Supermarket': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/Supermarket PoS 2018.xlsx'}
#     kpis = {'Pos 2018 - MT - Convenience Big': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/Convenience Big PoS 2018.xlsx'}
#     # 'Pos 2018 - MT - Convenience Big': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/Convenience Big PoS 2018.xlsx',
#     # 'Pos 2018 - HoReCa (Cofee /Tea Shops)': '/home/Shani/dev/trax_ace_factory/Projects/CCRU/Data/with weights/HoReCa Cofee_Tea Shops PoS 2018.xlsx'
#     # }
#     for name in kpis.keys():
#         kpi = CCRU_SANDAddKPIs(name, kpis[name])
#         kpi.add_fields_to_exist_kpis()
