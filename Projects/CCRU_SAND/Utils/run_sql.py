# -*- coding: utf-8 -*-

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers

__author__ = 'Sergey'


class CCRU_SANDRunSql:
    def __init__(self, project):
        self.project = project
        self.aws_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)

    def run_it(self):
        statement_1 = \
"""
UPDATE `static`.`atomic_kpi` SET `name`='CS@SSD Shelf Share', `description`='CS@SSD Shelf Share', `display_text`='SSD Shelf Share' WHERE `pk`='4704';
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



