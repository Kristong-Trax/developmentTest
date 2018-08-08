
__author__ = 'Nimrod'


class DISPLAYSQueries(object):

    @staticmethod
    def get_all_kpi_data():
        return """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.atomic_kpi api
            left join static.kpi kpi on kpi.pk = api.kpi_fk
            join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
            where kps.name = 'Manufacturer Displays'
        """

    @staticmethod
    def get_attributes_data():
        return """
            select pk as product_fk, att4
            from static.product
        """

    @staticmethod
    def get_match_display(session_uid):
        return """
            select d.display_name, m.scene_fk, m.bay_number
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
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
    def get_atomic_pk_to_delete(session_uid, set_fk):
        return '''SELECT k.pk FROM report.kpi_results k join static.kpi kpi on kpi.pk = k.kpi_fk
                where kpi.kpi_set_fk = '{}' and session_uid = '{}';'''.format(set_fk, session_uid)

    @staticmethod
    def get_kpi_pk_to_delete(session_uid, set_fk):
        return '''SELECT k.pk FROM report.kpk_results k join static.kpi kpi on kpi.pk = k.kpi_fk
                    where  kpi.kpi_set_fk = '{}' and session_uid = '{}';'''.format(set_fk, session_uid)

    @staticmethod
    def get_delete_session_results_query(session_uid, set_fk, kpi_pks, atomic_pks):
        if set_fk is not None and atomic_pks is not None and kpi_pks is not None:
            return ('''delete from report.kps_results where session_uid = '{}'
                            and kpi_set_fk = '{}' ;'''.format(session_uid, set_fk),
                    '''delete from report.kpk_results where session_uid = '{}' and pk in {};'''.format(session_uid,
                                                                                                       kpi_pks),
                    '''delete from report.kpi_results where session_uid = '{}' and pk in {} ;'''.format(session_uid,
                                                                                                        atomic_pks))