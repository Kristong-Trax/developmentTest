# -*- coding: utf-8 -*-

import pandas as pd
import os
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Cloud.Services.Connector.Keys import DbUsers

__author__ = 'Nimrod'


class MARSRU_PRODConsts(object):

    SET_NAME = 'KPI Level 1 Name'
    KPI_NAME = 'KPI Level 2 Name'
    KPI_WEIGHT = 'KPI Level 2 Weight'
    ATOMIC_NAME = 'KPI Level 3 Name'
    ATOMIC_DISPLAY_TEXT = 'KPI Level 3 Display Text'
    ATOMIC_WEIGHT = 'KPI Level 3 Weight'
    SORTING = 'Sorting'
    KPI_ENG_NAME = 'KPI name Eng'
    KPI_RUS_NAME = 'KPI name Rus'
    CHANNEL = 'Channel'
    PRESENTATION_ORDER = 'presentation_order'


class MARSRU_PRODAddKPIs(MARSRU_PRODConsts):
    """
    This module writes all levels of KPIs to the DB, given a template.

    - The template file must include a unique row for every Atomic KPI (and ONLY Atomics)
    - Each row much include Set-Name, KPI-Name and Atomic-Name columns (configured in 'MARSRU_PRODConsts')
    - Optionally KPI-Weight, Atomic-Weight and Atomic-DisplayText may be included
    - For every level of KPI (Set, KPI or Atomic) - only the ones that do not already exist by their names to be added to the DB

    """

    def __init__(self, project, template_path, sheet_name):
        self.project = project
        self.aws_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)
        self.kpi_static_data = self.get_kpi_static_data()
        self.data = pd.read_excel(template_path, sheet_name=sheet_name)
        self.sets_added = {}
        self.kpis_added = {}
        self.kpi_counter = {'set': 0, 'kpi': 0, 'atomic': 0}

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = """
                select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk, api.presentation_order,
                       kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                       kps.name as kpi_set_name, kps.pk as kpi_set_fk
                from static.atomic_kpi api
                left join static.kpi kpi on kpi.pk = api.kpi_fk
                join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
                """
        kpi_static_data = pd.read_sql_query(query, self.aws_conn.db)
        return kpi_static_data

    @staticmethod
    def get_relevant_file_for_set(set_name, data_path):
        """

        :param set_name: Channel attribute from self.data
        :param data_path: The directory with the project's templates
        :return: The relevant filename from the directory
        """
        words_to_include = list(['2018', 'POS'])
        words_to_include.append(set_name.upper())
        for filename in os.listdir(data_path):
            if all(word in filename.upper() for word in words_to_include):
                return filename
        Log.error("Couldn't find relevant file for the current set name = {}").format(set_name)
        return None

    def get_relevant_kpi_data_and_template_data(self):
        """
        This functions responsible to access and filter only the relevant data.
        In order to get the template name it iterates the relevant directory (PROD / SAND) and extract the relevant name
        In order to get the set name it iterates the relevant unique set name.
        :return: A tuple of the relevant KPI data and relevant project's template data
        """
        if self.project == 'ccru':
            dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        else:
            dir_path = os.path.join(os.path.dirname(os.path.realpath('..')), 'MARSRU_PROD', 'Data')

        # Get the relevant project template
        template_channel_name = self.data[MARSRU_PRODConsts.CHANNEL].values[0]
        if not template_channel_name:
            Log.error("The isn't a correct set in Channel attribute in the uploaded template.")
            return pd.DataFrame.empty,  pd.DataFrame.empty
        project_template_name = self.get_relevant_file_for_set(template_channel_name, dir_path)
        if not project_template_name:
            return pd.DataFrame.empty, pd.DataFrame.empty
        path = os.path.join(dir_path, project_template_name)
        project_current_data = pd.read_excel(path)

        # Get the relevant set name
        sets_2018 = [set_name for set_name in self.kpi_static_data['kpi_set_name'].unique()
                     if '2018' in set_name]
        current_set = [set_name for set_name in sets_2018 if
                       template_channel_name in set_name and 'SPIRITS' not in set_name.upper()]
        if not current_set:
            Log.error("Couldn't find relevant set for the current channel name = {}".format(
                template_channel_name))
            return pd.DataFrame.empty, pd.DataFrame.empty
        current_set = current_set[0]
        # Get the relevant KPI data and project template data
        relevant_kpi_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == current_set]
        filtered_project_data = project_current_data[
            project_current_data[MARSRU_PRODConsts.SORTING].isin(self.data[MARSRU_PRODConsts.SORTING].unique().tolist())]
        return relevant_kpi_data, filtered_project_data

    def update_atomic_kpi_data(self):
        """
        This function updates the kpi name, description and display_text according to the template.
        It reads the relevant set template, filters the relevant rows that need to be change and than collect and
        execute the queries in the DB.
        ** Note 1: It doesn't update the template itself!! **
        ** Note 2: Use regular excel without any filters!!
        """
        queries_to_execute = []
        update_query = "UPDATE static.atomic_kpi SET name='{}', description='{}', display_text='{}' where pk={};"
        # Get the data
        relevant_kpi_data, filtered_project_data = self.get_relevant_kpi_data_and_template_data()
        if relevant_kpi_data.empty:
            return

        # Iterate over the uploaded template and create the queries
        for i in xrange(len(self.data)):
            row = self.data.iloc[i]
            new_name = row[MARSRU_PRODConsts.KPI_ENG_NAME].replace('\n', '').strip()
            display = row[MARSRU_PRODConsts.KPI_RUS_NAME].replace('\n', '').strip()
            presentation_order = row[MARSRU_PRODConsts.SORTING]
            old_atomic_df = relevant_kpi_data[relevant_kpi_data[MARSRU_PRODConsts.PRESENTATION_ORDER]
                                              == presentation_order]
            if len(old_atomic_df) > 1:
                old_atomic_name = filtered_project_data[filtered_project_data[MARSRU_PRODConsts.SORTING] == presentation_order][
                    MARSRU_PRODConsts.KPI_ENG_NAME].values[0]
                try:
                    old_atom_fk = old_atomic_df[old_atomic_df['atomic_kpi_name'] == old_atomic_name][
                        'atomic_kpi_fk'].values[0]
                    queries_to_execute.append(
                        update_query.format(new_name, new_name, display.encode('utf-8'), old_atom_fk))
                except Exception as e:
                    Log.warning("No KPI defines for name = {}.".format(old_atomic_name))
                    continue
            else:
                old_atom_fk = old_atomic_df['atomic_kpi_fk'].values[0]
                queries_to_execute.append(update_query.format(
                    new_name, new_name, display.encode('utf-8'), old_atom_fk))

        # Execute the queries
        if queries_to_execute:
            cur = self.aws_conn.db.cursor()
            for query in queries_to_execute:
                cur.execute(query)
                print query
        self.aws_conn.db.commit()
        Log.info("Total number of executed queries = {}".format(len(queries_to_execute)))

    def update_kpi_weights(self):
        """
        This function updates KPI-Weights in the DB according to KPI-Weight column in the template.
        """
        update_query = "update static.kpi set weight = '{}' where pk = {}"
        queries = []
        for kpi in self.data.to_dict('records'):
            kpi_fk = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == kpi.get(self.SET_NAME)) &
                                          (self.kpi_static_data['kpi_name'] == kpi.get(self.KPI_NAME))]['kpi_fk'].values[0]
            queries.append(update_query.format(kpi.get(self.KPI_WEIGHT), kpi_fk))
        if queries:
            cur = self.aws_conn.db.cursor()
            for query in queries:
                cur.execute(query)
        self.aws_conn.db.commit()

    def update_atomic_weights(self):
        """
        This function updates Atomic-Weights in the DB according to KPI-Weight column in the template.
        """
        update_query = "update static.atomic_kpi set atomic_weight = '{}' where pk = {}"
        queries = []
        for atomic in self.data.to_dict('records'):
            atomic_fk = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == atomic.get(self.SET_NAME)) &
                                             (self.kpi_static_data['kpi_name'] == atomic.get(self.KPI_NAME)) &
                                             (self.kpi_static_data['atomic_kpi_name'] == atomic.get(self.ATOMIC_NAME))]['atomic_kpi_fk'].values[0]
            queries.append(update_query.format(kpi.get(self.ATOMIC_WEIGHT), atomic_fk))
        if queries:
            cur = self.aws_conn.db.cursor()
            for query in queries:
                cur.execute(query)
        self.aws_conn.db.commit()

    def add_kpis_from_template(self):
        self.add_sets_to_static()
        self.add_kpis_to_static()
        self.add_atomics_to_static()
        Log.info('{} Sets, {} KPIs and {} Atomics have been added'
                 .format(self.kpi_counter['set'], self.kpi_counter['kpi'], self.kpi_counter['atomic']))

    def add_sets_to_static(self):
        set_names = self.data[self.SET_NAME].unique().tolist()
        existing_set_names = self.kpi_static_data['kpi_set_name'].unique().tolist()
        set_names_to_add = set(set_names).difference(existing_set_names)
        if set_names_to_add:
            cur = self.aws_conn.db.cursor()
            for set_name in set_names_to_add:
                level1_query = \
                    """
                    INSERT INTO static.kpi_set (name, missing_kpi_score, enable, normalize_weight, expose_to_api, is_in_weekly_report)
                    VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');
                    """.format(set_name.encode('utf-8'), 'Bad', 'Y', 'N', 'N', 'N')
                cur.execute(level1_query)
                self.sets_added[set_name.encode('utf-8')] = cur.lastrowid
                self.kpi_counter['set'] += 1
            self.aws_conn.db.commit()
        set_names_ignored = set(set_names).difference(set_names_to_add)
        if set_names_ignored:
            Log.info("KPI Sets '{}' already exist. Ignored".format(set_names_ignored))

    def add_kpis_to_static(self):
        kpis = self.data.drop_duplicates(subset=[self.SET_NAME, self.KPI_NAME], keep='first')
        cur = self.aws_conn.db.cursor()
        for i in xrange(len(kpis)):
            set_name = kpis.iloc[i][self.SET_NAME].replace("'", "\\'").encode('utf-8')
            kpi_name = str(kpis.iloc[i][self.KPI_NAME]).replace("'", "\\'").encode('utf-8')
            if self.KPI_WEIGHT in kpis.iloc[i].keys():
                kpi_weight = float(kpis.iloc[i][self.KPI_WEIGHT])
            else:
                kpi_weight = 'NULL'
            if self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                    (self.kpi_static_data['kpi_name'] == kpi_name)].empty:
                if set_name in self.sets_added.keys():
                    set_fk = self.sets_added[set_name]
                else:
                    try:
                        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name']
                                                      == set_name]['kpi_set_fk'].values[0]
                    except:
                        set_fk = self.sets_added[set_name]
                level2_query = \
                    """
                    INSERT INTO static.kpi (kpi_set_fk, display_text, weight)
                    VALUES ('{0}', '{1}', {2});
                    """.format(set_fk, kpi_name, kpi_weight)
                cur.execute(level2_query)
                if set_name in self.kpis_added.keys():
                    self.kpis_added[set_name][kpi_name] = cur.lastrowid
                else:
                    self.kpis_added[set_name] = {kpi_name: cur.lastrowid}
                self.kpi_counter['kpi'] += 1
            else:
                Log.info("KPI '{}' already exists for KPI Set '{}'. Ignored".format(kpi_name, set_name))

        self.aws_conn.db.commit()

    def add_atomics_to_static(self):
        atomics = self.data.drop_duplicates(
            subset=[self.SET_NAME, self.KPI_NAME, self.ATOMIC_NAME], keep='first')
        cur = self.aws_conn.db.cursor()
        for i in xrange(len(atomics)):
            atomic = atomics.iloc[i]
            set_name = atomic[self.SET_NAME].replace("'", "\\'").encode('utf-8')
            kpi_name = str(atomic[self.KPI_NAME]).replace("'", "\\'").encode('utf-8')
            atomic_name = str(atomic[self.ATOMIC_NAME]).replace("'", "\\'").encode('utf-8')
            if self.ATOMIC_WEIGHT in atomics.iloc[i].keys():
                atomic_weight = float(atomics.iloc[i][self.ATOMIC_WEIGHT])
            else:
                atomic_weight = 'NULL'
            if self.ATOMIC_DISPLAY_TEXT in atomics.iloc[i].keys():
                atomic_display_text = atomics.iloc[i][self.ATOMIC_DISPLAY_TEXT].replace(
                    "'", "\\'").encode('utf-8')
            else:
                atomic_display_text = atomic_name

            if self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                    (self.kpi_static_data['kpi_name'] == kpi_name) &
                                    (self.kpi_static_data['atomic_kpi_name'] == atomic_name)].empty:
                if set_name in self.kpis_added.keys() and kpi_name in self.kpis_added[set_name].keys():
                    kpi_fk = self.kpis_added[set_name][kpi_name]
                else:
                    kpi_fk = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                                  (self.kpi_static_data['kpi_name'] == kpi_name)]['kpi_fk'].values[0]

                level3_query = \
                    """
                    INSERT INTO static.atomic_kpi (kpi_fk, name, description, display_text, presentation_order, display, atomic_weight)
                    VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}');
                    """.format(kpi_fk, atomic_name, atomic_name, atomic_display_text, 1, 'Y', atomic_weight)
                cur.execute(level3_query)
                self.kpi_counter['atomic'] += 1
            else:
                Log.info("Atomic '{}' already exists for KPI '{}' Set '{}'. Ignored".format(
                    atomic_name, kpi_name, set_name))
        self.aws_conn.db.commit()


if __name__ == '__main__':
    LoggerInitializer.init('Add KPIs to static')
    # Config.init()
    # docker_user = DbUsers.Docker
    # dbusers_class_path = 'Trax.Utils.Conf.Keys'
    # dbusers_patcher = patch('{0}.DbUser'.format(dbusers_class_path))
    # dbusers_mock = dbusers_patcher.start()
    # dbusers_mock.return_value = docker_user
    kpi = MARSRU_PRODAddKPIs(
        'marsru-prod', '/home/sergey/dev/kpi_factory/Projects/MARSRU_PROD/Data/2019/KPIs for DB - MARS KPIs.xlsx', 'Sheet4')
    kpi.add_kpis_from_template()
    # kpi.update_atomic_kpi_data()
    # kpi.update_kpi_weights()
    # kpi.update_atomic_weights()
