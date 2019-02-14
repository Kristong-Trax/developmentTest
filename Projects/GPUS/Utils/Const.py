

__author__ = 'Nic, Sam'


class Const(object):
    FACINGS = 'facings'
    FACINGS_IGN_STACKING = 'facings_ign_stack'
    LINEAR_FACINGS = 'net_len_ign_stack'
    ALLOWED_EDGES = {'left', 'right'}
    EMPTY_FILTER = {'product_type': 'Empty'}
    EMPTY_FKS = {'Empty': 0}
    HIERARCHY = {'brand': {'parent': 'category', 'ident': None}, 'category': {'parent': None, 'ident': 'category'}}
    IRRELEVANT = 'Irrelevant'
    GENERAL = 'General'
    KEEP = 'keep'
    KPI_FAMILY = 'KPI Family'
    LINEAR_SOS_KPI = 'PS_LINEAR_SOS'
    FACING_SOS_KPI = 'PS_FACINGS_SOS'
    PS_PARENT = 'PS_PARENT'
    ASSORTMENT = 'ASSORTMENT'
    OOS = 'OOS'
    SHARE_OF_EMPTY_KPI = 'PS_SHARE_OF_EMPTY'
    NUMERATOR = 'Numerator'
    DENOMINATOR = 'Denominator'
    EDGES = ['left', 'right']
    MPIS_COLS = ['scene_fk', 'product_fk', 'stacking_layer', 'width_mm_advance']
    SUM_COLS = {
                LINEAR_SOS_KPI: LINEAR_FACINGS,
                FACING_SOS_KPI: FACINGS,
                SHARE_OF_EMPTY_KPI: FACINGS_IGN_STACKING
                }


    SOS_HIERARCHY = {
                    'Brand out of Category Facings SOS': 'Manufacturer out of Category Facings SOS',
                    'Manufacturer out of Category Facings SOS': 'Manufacturer out of Store Facings SOS',
                    'Manufacturer out of Store Facings SOS': 'Total Facings SOS',

                    'Brand out of Category Linear SOS': 'Manufacturer out of Category Linear SOS',
                    'Manufacturer out of Category Linear SOS': 'Manufacturer out of Store Linear SOS',
                    'Manufacturer out of Store Linear SOS': 'Total Linear SOS',

                    'Share of Empty out of Category': 'Share of Empty out of Store',
                    'Share of Empty out of Store': 'Total Share of Empty',
                    }

    KPI_QUERY = '''
                select kpi.*, 
                kpif.name as 'KPI Family',
                n_ent.name as 'Numerator',
                d_ent.name as 'Denominator'
                from static.kpi_level_2 kpi
                left join static.kpi_family kpif on kpi.kpi_family_fk = kpif.pk
                left join static.kpi_entity_type n_ent on kpi.numerator_type_fk = n_ent.pk
                left join static.kpi_entity_type d_ent on kpi.denominator_type_fk = d_ent.pk
                '''