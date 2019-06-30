
__author__ = 'Nethanel'


class MarsUsQueries(object):

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
    def get_store_attribute(attribute, store_fk):
        return """
                    select additional_attribute_{} from static.stores
                    where pk = {}
                    """.format(attribute, store_fk)

    @staticmethod
    def get_store_retailer(store_fk):
        return """
                    select R.name from static.stores S
                    join static.retailer R
                    on S.retailer_fk = R.pk
                    where S.pk =  {}
                    """.format(store_fk)

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
    def add_kpi_to_mvp_sr(kpi, pk):
        return '''
        insert into static.match_product_in_probe_state_reporting
        (pk, name, display_name)
        values
        ({}, "{}", "{}")
        '''.format(pk, kpi, kpi)

    @staticmethod
    def get_updated_mvp_sr():
        return 'select * from static.match_product_in_probe_state_reporting'
