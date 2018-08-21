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
update static.stores
set additional_attribute_11 = 'Pos 2018 - HoReCa - Restaurant Cafe'
where additional_attribute_11 = 'Pos 2018 - HoReCa - Restaurant Café';


                        """

        # """
        # UPDATE `static`.`atomic_kpi` SET `name`='Cooler: Max 26', `description`='Cooler: Max 26', `display_text`='Cooler: Max 26' WHERE `pk` in ('3262','3392','3511','3889','3998','4178');
        #
        # UPDATE `static`.`kpi` SET `display_text`='Cooler: Max 26' WHERE `pk` in ('2511','2535','2557','2621','2655','2686');
        #
        # """

        cur = self.aws_conn.db.cursor()
        cur.execute(statement_1)
        self.aws_conn.db.commit()


if __name__ == '__main__':
    Log.init('test')
    Config.init()
    for project in ['ccru']:
        print 'start project: ' + str(project)
        sql_to_run = CCRU_SANDRunSql(project)
        sql_to_run.run_it()



