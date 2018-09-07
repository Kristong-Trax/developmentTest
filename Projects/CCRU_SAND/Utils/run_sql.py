# -*- coding: utf-8 -*-

from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers

__author__ = 'Sergey'


class CCRU_SANDRunSql:
    def __init__(self, project):
        self.project = project
        self.aws_conn = AwsProjectConnector(self.project, DbUsers.CalculationEng)

    def run_it(self):
        statement_1 = \
"""
UPDATE `static`.`kpi_set` SET `name`='Contract Execution 2018 Irrelevant' WHERE `pk`='56';
"""
# """
# DELETE FROM `static`.`kpi_set` WHERE `pk`>='64';
# """
# """
# DELETE FROM `static`.`kpi` WHERE `pk`>='2843';
# """
# """
# UPDATE `static`.`kpi_set` SET `name`='Contract Execution 2018 X' WHERE `pk`='56';
# """

        cur = self.aws_conn.db.cursor()
        cur.execute(statement_1)
        self.aws_conn.db.commit()


if __name__ == '__main__':
    Log.init('Run SQL')
    Config.init()
    for project in ['ccru_sand']:
        print 'Start Project: ' + str(project)
        sql_to_run = CCRU_SANDRunSql(project)
        sql_to_run.run_it()



