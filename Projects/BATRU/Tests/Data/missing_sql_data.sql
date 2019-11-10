INSERT INTO `static`.`kpi_entity_type`
(`pk`, `name`, `table_name`, `uid_field`, `parent_entity_type_fk`, `parent_id_column`)
VALUES ('15', 'equipment_type', 'static.template', NULL, NULL, NULL),
('16', 'scene_data', 'probedata.scene', NULL, NULL, NULL),
('17', 'display_group', 'static.custom_entity', NULL, NULL, NULL),
('18', 'display_type_in_group', 'static.custom_entity', NULL, NULL, NULL),
('19', 'kpi_name', 'static.kpi_level_2', NULL, NULL, NULL),
('20', 'section', 'static.custom_entity', NULL, NULL, NULL);


INSERT INTO `static`.`kpi_result_type`
(`pk`, `name`, `kpi_scale_type_fk`)
VALUES ('2', 'CUSTOM_OSA', '1'), ('3', 'CUSTOM_DATE', '1');


INSERT INTO `static`.`kpi_result_value`
(`pk`, `value`, `kpi_result_type_fk`)
VALUES ('4', 'OOS contracted', '2'), ('5', 'No Distribution', '2'),
('6', 'AV', '2'), ('7', 'OOS rest', '2'), ('8', 'NA', '2'),
('9', '0', '3');