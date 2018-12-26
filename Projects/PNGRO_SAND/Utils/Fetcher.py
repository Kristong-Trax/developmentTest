
__author__ = 'Israel'

class PNGRO_SAND_PRODQueries(object):
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
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))

    @staticmethod
    def get_match_display(session_uid):
        return """
                select sdb.name, m.scene_fk, d.display_name, m.bay_number, m.rect_x, m.rect_y, d.pk
                from probedata.match_display_in_scene m
                join probedata.scene s on s.pk = m.scene_fk
                join static.display d on d.pk = m.display_fk
                join static.display_brand sdb on sdb.pk=d.display_brand_fk
                join 	(
    					select A.pk, A.scene_fk, A.creation_time
    					from probedata.match_display_in_scene_info A
    					join (	select scene_fk, max(creation_time) as creation_time
    							from probedata.match_display_in_scene_info
    							where delete_time is NULL
    							group by scene_fk
    						) B
    					on   A.scene_fk=B.scene_fk
    					and A.creation_time = B.creation_time
                        ) mdici
                on m.match_display_in_scene_info_fk=mdici.pk
                where s.session_uid = '{}'
            """.format(session_uid)

    @staticmethod
    def get_match_stores_by_retailer():
        return """
            select s.pk, r.name
            from static.stores s
            inner join static.retailer r
            on s.retailer_fk = r.pk
        """

    @staticmethod
    def get_template_fk_by_category_fk():
        return """
                select pk, product_category_fk from static.template
                where product_category_fk is not null
            """

    @staticmethod
    def get_status_session_by_display(session_uid):
        return """
        select SD.exclude_status_fk
        from probedata.session_display SD
        inner join probedata.session S
        on SD.session_fk = S.pk
        where S.session_uid = '{}'
        """.format(session_uid)

    @staticmethod
    def get_status_session_by_category(session_uid):
        return """
        select SC.category_fk
        from probedata.session_category SC
        inner join probedata.session S
        on SC.session_fk = S.pk
        where SC.exclude_status_fk in (1,4)
        and S.session_uid = '{}'
        """.format(session_uid)

    @staticmethod
    def get_test_query(session_uid):
        return """
        select *
        from probedata.session
        where session_uid = '{}'
        """.format(session_uid)