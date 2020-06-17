__author__ = 'Nimrod'


class PNGJP_SAND2Queries(object):
    @staticmethod
    def get_all_kpi_data():
        return """
            select kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk, a.name as atomic_kpi_name, a.pk as atomic_kpi_fk
            from static.kpi_set kps
            join static.kpi kpi on kps.pk = kpi.kpi_set_fk
            left join static.atomic_kpi a on a.kpi_fk=kpi.pk
        """

    @staticmethod
    def get_match_display(session_uid):
        return """
            select d.display_name, d.pk as display_fk, m.scene_fk, sdt.name as display_type
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            join static.display_type sdt on d.display_type_fk=sdt.pk
            where s.session_uid = '{}'
        """.format(session_uid)

    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))

    @staticmethod
    def get_delete_session_custom_scif(session_fk):
        query = "delete from pservice.custom_scene_item_facts where session_fk = '{}';".format(session_fk)
        return query

    @staticmethod
    def get_probe_group(session_uid):
        return """
                select distinct
                sub_group_id probe_group_id,
                mpip.pk probe_match_fk
                from
                probedata.stitching_probe_info as spi
                inner join probedata.stitching_scene_info ssi on
                ssi.pk = spi.stitching_scene_info_fk
                inner join probedata.match_product_in_probe mpip on
                mpip.probe_fk = spi.probe_fk
                inner join probedata.scene as sc on
                sc.pk = ssi.scene_fk
                where ssi.delete_time is null
                and sc.session_uid = '{}';
        """.format(session_uid)

    @staticmethod
    def get_kpi_external_targets(visit_date):
        return """SELECT ext.*, ot.operation_type, kpi.type as kpi_type
                      FROM static.kpi_external_targets ext
                      LEFT JOIN static.kpi_operation_type ot on ext.kpi_operation_type_fk=ot.pk
                      LEFT JOIN static.kpi_level_2 kpi on ext.kpi_level_2_fk = kpi.pk
                      WHERE (ext.start_date<='{}' and ext.end_date is null) or 
                      (ext.start_date<='{}' and ext.end_date>='{}')""".format(visit_date, visit_date, visit_date)