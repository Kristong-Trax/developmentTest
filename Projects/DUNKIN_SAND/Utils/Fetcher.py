
__author__ = 'ortalk'


class DUNKINQueries:

    def __init__(self):
        pass

    @staticmethod
    def get_all_kpi_data():
        return """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.kpi_set kps
            left join static.kpi kpi on kpi.kpi_set_fk = kps.pk
            left join static.atomic_kpi api on api.kpi_fk = kpi.pk
        """


    @staticmethod
    def get_delete_session_results_query(session_uid,set_fk):
        if set_fk is not None:
            return ("delete from report.kps_results where session_uid = '{}' "
                    "and kpi_set_fk = '{}' ;".format(session_uid, set_fk),
                    "delete from report.kpk_results kpk"
                    "join static.kpi kpi on kpi.pk = kpk.kpi_fk  where session_uid = '{}' "
                    "and kpi.kpi_set_fk = '{}';".format(session_uid, set_fk),
                    "delete from report.kpi_results kpir"
                    "join static.kpi kpi on kpi.pk = kpir.kpi_fk "
                    "where session_uid = '{}' and kpi.kpi_set_fk = '{}' ;".format(session_uid,set_fk))
        else:
            return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                    "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                    "delete from report.kpi_results where session_uid = '{}';".format(session_uid))

    @staticmethod
    def get_match_display(session_uid):
        return """
            select sdb.name, m.scene_fk, d.display_name, m.bay_number, m.rect_x, m.rect_y,dt.name as display_type
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            join static.display_brand sdb on sdb.pk=d.display_brand_fk
            join static.display_type dt on dt.pk = d.display_type_fk
            where s.session_uid = '{}'
        """.format(session_uid)


    @staticmethod
    def get_product_atts():
        return """
            SELECT att4,att3,product_ean_code from
            static.product
        """