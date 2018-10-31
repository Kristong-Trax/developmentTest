# -*- coding: utf-8 -*-

from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers

__author__ = 'Sergey'


class MOLSONCOORSRS_SANDRunSql:
    def __init__(self, project):
        self.project = project
        self.aws_conn = AwsProjectConnector(self.project, DbUsers.CalculationEng)

    def run_it(self):
        statements = \
"""
UPDATE `static`.`kpi_level_2` SET `client_name`='SOS Cooler' WHERE `pk`='1';
UPDATE `static`.`kpi_level_2` SET `client_name`='SOS Ambient' WHERE `pk`='2';

"""
        # """
        # DELETE FROM `pservice`.`kpi_targets`;
        # INSERT INTO `static`.`kpi_calculation_stage` (`pk`, `name`) VALUES ('3', 'PS Customer Specific');
        # INSERT INTO `static`.`kpi_family` (`pk`, `name`, `kpi_calculation_stage_fk`) VALUES ('21', 'assortment', '3');
        # INSERT INTO `static`.`kpi_family` (`pk`, `name`, `kpi_calculation_stage_fk`) VALUES ('22', 'sos', '3');
        # INSERT INTO `static`.`kpi_level_2` (`pk`, `type`, `client_name`, `kpi_family_fk`, `version`, `numerator_type_fk`, `denominator_type_fk`, `kpi_calculation_stage_fk`, `session_relevance`, `scene_relevance`, `planogram_relevance`, `live_session_relevance`, `live_scene_relevance`, `is_percent`) VALUES ('1', 'SOS_manufacturer_category_cooler', 'SOS Cooler', '22', '1', '3', '4', '3', '1', '0', '0', '0', '0', '0');
        # INSERT INTO `static`.`kpi_level_2` (`pk`, `type`, `client_name`, `kpi_family_fk`, `version`, `numerator_type_fk`, `denominator_type_fk`, `kpi_calculation_stage_fk`, `session_relevance`, `scene_relevance`, `planogram_relevance`, `live_session_relevance`, `live_scene_relevance`, `is_percent`) VALUES ('2', 'SOS_manufacturer_category_ambient', 'SOS Ambient', '22', '1', '3', '4', '3', '1', '0', '0', '0', '0', '0');

        # INSERT INTO `static`.`kpi_entity_type` (`pk`, `name`, `table_name`) VALUES ('1010', 'assortment', 'pservice.assortment');

        # """

        for statement in statements.split('\n'):
            if len(statement):
                cur = self.aws_conn.db.cursor()
                print statement
                cur.execute(statement)
                self.aws_conn.db.commit()


if __name__ == '__main__':
    Log.init('Run SQL')
    Config.init()
    for project in ['molsoncoorsrs_sand']:
        print 'Start Project: ' + str(project)
        sql_to_run = MOLSONCOORSRS_SANDRunSql(project)
        sql_to_run.run_it()



