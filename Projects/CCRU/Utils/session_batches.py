# -*- coding: utf-8 -*-
import pandas as pd

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Conf.Configuration import Config
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

__author__ = 'Sergey'

PROJECT = 'ccru'
START_DATE = '2019-06-29'
END_DATE = '2019-07-12'
NUMBER_OF_SCENES_LIMIT = 10000
BATCH_FILE = '/home/sergey/Documents/Recalc/' + PROJECT + '_sessions_'


class CCRUSessionBatches:
    def __init__(self, project):
        self.project = project
        self.aws_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)

    def run_it(self):
        # query = """
        #         SELECT visit_date, session_uid, number_of_scenes
        #         FROM probedata.session
        #         WHERE number_of_scenes > 0
        #         AND visit_date >= '{}' AND visit_date <= '{}'
        #         AND (external_session_id NOT LIKE 'EasyMerch-P%' OR external_session_id IS NULL)
        #         ORDER BY visit_date
        #         """.format(START_DATE, END_DATE)
        query = """
                SELECT ss.visit_date, ss.session_uid, ss.number_of_scenes
                FROM probedata.session ss
                JOIN report.kps_results ksr ON ksr.session_uid=ss.session_uid
                JOIN static.kpi_set ks ON ks.pk=ksr.kpi_set_fk
                WHERE ss.number_of_scenes > 0
                AND ss.visit_date >= '{}' AND ss.visit_date <= '{}'
                AND (ss.external_session_id NOT LIKE 'EasyMerch-P%' OR ss.external_session_id IS NULL)
                AND ks.name IN(
    'PoS 2019 - FT - CAP',
    'PoS 2019 - FT - REG',
    'PoS 2019 - FT NS - CAP',
    'PoS 2019 - FT NS - REG',
    'PoS 2019 - IC Petroleum - CAP',
    'PoS 2019 - IC Petroleum - REG',
    'PoS 2019 - IC FastFood',
    'PoS 2019 - MT Conv Big - CAP',
    'PoS 2019 - MT Conv Big - REG',
    'PoS 2019 - MT Conv Small - CAP',
    'PoS 2019 - MT Conv Small - REG',
    'PoS 2019 - MT Hypermarket - CAP',
    'PoS 2019 - MT Hypermarket - REG',
    'PoS 2019 - MT Supermarket - CAP',
    'PoS 2019 - MT Supermarket - REG',
    'Contract Execution 2019'
                )
                ORDER BY ss.visit_date;
                """.format(START_DATE, END_DATE)

        sessions = pd.read_sql_query(query, self.aws_conn.db)

        batch_number = 1
        scene_counter = 0
        session_counter = 0
        total_counter = 0
        batch_sessions = []
        batch_file = open(BATCH_FILE + str(batch_number) + '.csv', 'w+')

        for i, row in sessions.iterrows():

            session_uid = row['session_uid']
            number_of_scenes = row['number_of_scenes']

            if number_of_scenes > NUMBER_OF_SCENES_LIMIT:
                print 'Session {} contains number of scenes {} which above the limit {}' \
                      ''.format(number_of_scenes, number_of_scenes, NUMBER_OF_SCENES_LIMIT)

            if scene_counter + number_of_scenes >= NUMBER_OF_SCENES_LIMIT:
                batch_file.writelines(batch_sessions)
                batch_file.close()
                print 'File {}: {} sessions'.format(BATCH_FILE + str(batch_number) + '.csv', session_counter)

                batch_number += 1
                scene_counter = 0
                session_counter = 0
                batch_sessions = []
                batch_file = open(BATCH_FILE + str(batch_number) + '.csv', 'w+')

            batch_sessions.append(session_uid + '\n')
            scene_counter += number_of_scenes
            session_counter += 1
            total_counter += 1

        batch_file.writelines(batch_sessions)
        batch_file.close()
        print 'File {}: {} sessions'.format(BATCH_FILE + str(batch_number), session_counter)
        print 'Total: {} sessions'.format(total_counter)


if __name__ == '__main__':
    LoggerInitializer.init('Run Session Batches')
    Config.init()
    session_batches_run = CCRUSessionBatches(PROJECT)
    session_batches_run.run_it()



