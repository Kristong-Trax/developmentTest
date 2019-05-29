
__author__ = 'ilays'

class INBEVMXQueries(object):

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
    def get_delete_session_results_query(session_uid, session_id):
        return ("delete from report.kpi_level_2_results where session_fk = '{}' and (kpi_level_2_fk "
                "in (select pk from static.kpi_level_2 where kpi_calculation_stage_fk = '3'));".format(session_id))


    @staticmethod
    def get_policies():
        return """ select p.policy_name, p.policy, atag.assortment_group_fk, atp.assortment_fk, atp.product_fk, 
		            atp.start_date, atp.end_date from pservice.assortment_to_product atp 
                    join pservice.assortment_to_assortment_group atag on atp.assortment_fk = atag.assortment_fk 
                    join pservice.assortment a on a.pk = atag.assortment_group_fk
					join pservice.policy p on p.pk = a.store_policy_group_fk;
                """