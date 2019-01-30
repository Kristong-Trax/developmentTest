# -*- coding: utf-8 -*-
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers

__author__ = 'Sergey'


class MARSRU_PRODRunSql:
    def __init__(self, project):
        self.project = project
        self.aws_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)

    def run_it(self):
        statement_1 = """
                       UPDATE `static`.`survey_question` SET `question_text`='Категория товаров для животных примыкает к ПРОМО АЛЛЕЕ, находится дальше 5-ти метров от входа и визуально доступна покупателям по ходу их движения без необходимости оборачиваться' WHERE `pk`='293';

                      """

        cur = self.aws_conn.db.cursor()
        cur.execute(statement_1)
        self.aws_conn.db.commit()


if __name__ == '__main__':
    Log.init('test')
    Config.init()
    for project in ['marsru2-sand']:
        print 'start project: ' + str(project)
        sql_to_run = MARSRU_PRODRunSql(project)
        sql_to_run.run_it()
