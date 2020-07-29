
__author__ = 'Nimrod'


class OBBOQueries(object):

    @staticmethod
    def get_all_kpi_data():
        return """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.atomic_kpi api
            left join static.kpi kpi on kpi.pk = api.kpi_fk
            join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
            where kps.name = 'OBBO'
        """

    @staticmethod
    def get_match_display(session_uid):
        return """
            select d.pk as display_fk, d.display_name, dt.name as display_type, m.scene_fk, m.bay_number
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            join static.display_type dt on dt.pk = d.display_type_fk
            join (select
                      s.pk as scene_fk,
                      max(m.creation_time) as creation_time
                  from probedata.match_display_in_scene m
                  join probedata.scene s on s.pk = m.scene_fk
                  where s.session_uid = '{session}'
                  group by s.pk) ct on ct.scene_fk = s.pk
            where s.session_uid = '{session}' and m.bay_number > 0 and ct.creation_time = m.creation_time
        """.format(session=session_uid)

    @staticmethod
    def get_missing_attributes_data():
        return """
            select pk as product_fk, att3
            from static.product
        """

    @staticmethod
    def get_delete_session_results_query(session_uid, kpi_static_data):
        kps_name = kpi_static_data['kpi_set_name'].values[0]
        kpi_ids = tuple(kpi_static_data['kpi_fk'].unique().tolist())
        return ("delete from report.kps_results where session_uid = '{}' and kps_name = '{}';".format(session_uid, kps_name),
                "delete from report.kpk_results where session_uid = '{}' and kpi_fk in {};".format(session_uid, kpi_ids),
                "delete from report.kpi_results where session_uid = '{}' and kpi_fk in {};".format(session_uid, kpi_ids))
