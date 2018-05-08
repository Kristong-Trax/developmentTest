SET FOREIGN_KEY_CHECKS = 0;

REPLACE INTO `static`.`kpi_family` VALUES  (1000000, 'CUSTOM_KPIS');

truncate static.kpi_level_2;
REPLACE INTO static.kpi_level_2
(pk, kpi_level_1_fk, type, client_name, kpi_family_fk, version, numerator_type_fk, denominator_type_fk, valid_from, valid_until, initiated_by, context_type_fk, kpi_result_type_fk, kpi_score_type_fk)
VALUES (100, null, 'MY_CUSTOM_CALCULATION', 'MY_CUSTOM_CALCULATION', 1000000, '1.0.0', 3, 5, '1990-01-01', '2100-01-01', 'PS', null, 1, null);


REPLACE INTO static.kpi_level_2_target(pk, kpi_level_2_fk, policy_fk, entity, start_date, end_date, target)
VALUES (1000000, 1000000, 1,  '{}', '2010-01-01', '2116-10-15', '20');

SET FOREIGN_KEY_CHECKS = 1;