__author__ = 'Sanad'


class MARS_CHOCO_RU_SANDMARSQueries(object):
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
        select m.scene_fk, d.display_name
        from probedata.match_display_in_scene m
        join probedata.scene s on s.pk = m.scene_fk join static.display d on d.pk = m.display_fk
            where s.session_uid = '{}'
        """.format(session_uid)

    @staticmethod
    def get_match_display_by_condition(session_uid):
        return """
               select d.display_name , s.pk as scene_id , man.name as manufacturer_name
               from probedata.match_display_in_scene m
               join probedata.scene s on s.pk = m.scene_fk
               join static.display d on d.pk = m.display_fk
               join static.display_type dt on dt.pk = d.display_type_fk
               join static.display_brand db on d.display_brand_fk = db.pk
               join static.brand b on b.pk = db.brand_fk
               join static.manufacturers man on man.pk = b.manufacturer_fk
               where s.session_uid = '{}'
               """.format(session_uid)
    @staticmethod
    def get_location(session_uid, location_name, scene_type ):
        return """
        select scene.pk as 'Scene Id' from probedata.scene_store_task_area_group_items scene_location
        join probedata.scene scene on scene.pk = scene_location.scene_fk
        join static.store_task_area_group_items location  on location.pk
        = scene_location.store_task_area_group_item_fk
        join static.template template on template.pk = scene.template_fk
        where scene.session_uid = '{}'
        and location.name in ({})
        and template.name in({})
		   """.format(session_uid, location_name, scene_type )