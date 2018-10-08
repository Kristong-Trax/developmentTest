
__author__ = 'urid'


class Queries:

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
    def get_business_unit_data(store_fk):
        return """
            select bu.name
            from static.stores st
            join static.business_unit bu on bu.pk = st.business_unit_fk
            where st.pk = {}
        """.format(store_fk)

    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))

    @staticmethod
    def get_match_display(session_uid):
        return """
            select sdb.name, m.scene_fk, d.display_name, m.bay_number, m.rect_x, m.rect_y
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            join static.display_brand sdb on sdb.pk=d.display_brand_fk
            join (
            select scene_fk, max(match_display_in_scene_info_fk) as max_match_display
            from probedata.match_display_in_scene m
            group by scene_fk
            ) temp on temp.max_match_display=m.match_display_in_scene_info_fk
            where s.session_uid = '{}'
        """.format(session_uid)

    @staticmethod
    def get_oos_messages(store_fk, session_uid):
        return """
            SELECT * FROM probedata.oos_exclude oe
            join static.oos_message om on om.pk=oe.oos_message_fk
            join static.oos_message_type omt on omt.pk=om.type
            where session_uid = '{}';
        """.format(session_uid)

    @staticmethod
    def get_osa_table(store_fk, visit_date, current_date, status):
        if str(status) == "Completed":
            return """
                SELECT * FROM pservice.custom_osa
                WHERE store_fk = {} 
                AND '{}' between IFNULL(start_date, '{}') and IFNULL(end_date, '{}') 
            """.format(store_fk, visit_date, current_date, current_date)
        else:

            return """
                SELECT * FROM pservice.custom_osa
                WHERE store_fk = {} AND is_current = 1
            """.format(store_fk)

    @staticmethod
    def get_delete_osa_records_query(product_fk, store_fk, delete_time, visit_date, current_date):
        return """
            UPDATE pservice.custom_osa
            SET end_date = '{}', is_current=NULL
            WHERE product_fk = {} AND store_fk = {}
            AND '{}' >= IFNULL(start_date, '{}')
            AND is_current = 1
        """.format(delete_time, product_fk, store_fk, visit_date, current_date, current_date)

    @staticmethod
    def get_delete_scif_records_query(product_fk, session_fk):
        return """
            UPDATE pservice.custom_scene_item_facts
            SET  in_assortment_osa=0, oos_osa=0
            WHERE product_fk = {} AND session_fk = {}
        """.format(product_fk, session_fk)

    @staticmethod
    def get_store_number_1(store_fk):
        return """
            SELECT store_number_1
            FROM static.stores
            WHERE pk = {}
        """.format(store_fk)

    @staticmethod
    def get_product_att4(product_fk):
        return """
            SELECT att4 from static.product
            WHERE pk = {}
        """.format(product_fk)

    @staticmethod
    def get_delete_custom_scif_query(session_id):
        return """
            delete from pservice.custom_scene_item_facts
            where session_fk = {}
        """.format(session_id)

    @staticmethod
    def get_att3_att4_for_products():
        return """
                select pk as product_fk,att3,att4 from static.product
                """

    @staticmethod
    def get_store_attribute_8(store_fk):
        return """
                select additional_attribute_8 from static.stores
                where pk = {}
                """.format(store_fk)

    @staticmethod
    def get_rect_values_query(session_uid):
        return """
            select mpis.pk as scene_match_fk,product_fk,scene_fk,bay_number,shelf_number ,rect_x,rect_y, width_mm_advance
            from probedata.match_product_in_scene mpis
            join probedata.scene sc on sc.pk=mpis.scene_fk
            where session_uid = '{}'
            """.format(session_uid)
