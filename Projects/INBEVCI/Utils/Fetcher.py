
__author__ = 'Elyashiv'


class INBEVCIINBEVCIQueries(object):

    @staticmethod
    def get_all_kpi_data():
        return """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.atomic_kpi api
            left join static.kpi kpi on kpi.pk = api.kpi_fk
            join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
        """

    @staticmethod
    def get_new_kpi_data():
        return """
                select *
                from static.kpi_level_2
                """

    @staticmethod
    def get_attribute5(session_uid):
        return """select st.additional_attribute_5 from probedata.session se join static.stores st on
        se.store_fk = st.pk where se.session_uid = "{}";""".format(session_uid)
    #
    # @staticmethod
    # def get_brand_fk(brand_name):
    #     return """select pk from static.brand where name = "{}";""".format(brand_name)

    @staticmethod
    def get_district_name(district_fk):
        return """select name from static.district where pk = {};""".format(district_fk)

    @staticmethod
    def get_groups_fk():
        return """select * from pservice.group_names;"""

    @staticmethod
    def insert_group_to_pservice(group_name):
        return """INSERT INTO pservice.group_names (group_name) VALUES ("{}");""".format(group_name)

    @staticmethod
    def get_match_display(session_uid):
        return """
                select sdb.name, m.scene_fk, d.display_name, m.bay_number, m.rect_x, m.rect_y
                from probedata.match_display_in_scene m
                join probedata.scene s on s.pk = m.scene_fk
                join static.display d on d.pk = m.display_fk
                join static.display_brand sdb on sdb.pk=d.display_brand_fk
                where s.session_uid = '{}'
            """.format(session_uid)

    @staticmethod
    def get_delete_session_results_query(session_id):
        return ("delete from report.kpi_level_2_results where session_fk = '{}' and (kpi_level_2_fk "
                "in (select pk from static.kpi_level_2 where kpi_calculation_stage_fk = '3'));".format(session_id))

    @staticmethod
    def get_store_policies():
        return """select ktp.*, p.policy as store_policy from
                    (select kt.kpi, kt.sku_name, kt.target, kt.target_validity_start_date, kt.target_validity_end_date, 
                            kt.store_policy_fk, p.policy as sos_policy 
                     from pservice.kpi_targets kt,
                          pservice.policy p
                     where kt.store_policy_fk = p.pk) as ktp,
                   pservice.policy p
                   where ktp.sku_name = p.policy_name;
                """

    @staticmethod
    def kpi_external_targets_query(operation_types, visit_date):
        if len(operation_types) == 1:
            query = """SELECT ext.*, kpi.type, ot.operation_type from static.kpi_external_targets ext
                              LEFT JOIN static.kpi_operation_type ot on ext.kpi_operation_type_fk=ot.pk
                              LEFT JOIN static.kpi_level_2 kpi on ext.kpi_level_2_fk = kpi.pk
                              WHERE
                              ((ext.start_date<='{}' and ext.end_date is null) or 
                              (ext.start_date<='{}' and ext.end_date>='{}'))
                              AND ot.operation_type='{}' order by ext.pk
                           """.format(visit_date, visit_date, visit_date, operation_types[0])
        else:
            query = """SELECT ext.*, kpi.type, ot.operation_type from static.kpi_external_targets ext
                                          LEFT JOIN static.kpi_operation_type ot on ext.kpi_operation_type_fk=ot.pk 
                                          LEFT JOIN static.kpi_level_2 kpi on ext.kpi_level_2_fk = kpi.pk
                                          WHERE
                                          ((ext.start_date<='{}' and ext.end_date is null) or 
                                          (ext.start_date<='{}' and ext.end_date>='{}'))
                                          AND ot.operation_type in {} order by ext.pk
                                       """.format(visit_date, visit_date, visit_date, tuple(operation_types))
        return query