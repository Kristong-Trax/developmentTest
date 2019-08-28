class Consts(object):
    SHELF_TOP = 'shelf_px_top'
    SHELF_BOTTOM = 'shelf_px_bottom'
    SHELF_LEFT = 'shelf_px_left'
    SHELF_RIGHT = 'shelf_px_right'

    FLEXIBLE_MODE = 'Flexible Mode'
    STRICT_MODE = 'Strict Mode'

    ATTRIBUTES_TO_SAVE = ['scene_match_fk','product_name', 'product_fk', 'product_type', 'product_ean_code', 'sub_brand_name',
                          'brand_name', 'category', 'sub_category', 'manufacturer_name', 'front_facing',
                          'category_local_name', 'shelf_number', SHELF_TOP, SHELF_BOTTOM, SHELF_LEFT, SHELF_RIGHT, 'x_mm', 'y_mm',
                          'bay_number', 'width_mm_advance', 'height_mm_advance']
    KPI_RESULT = 'report.kpi_results'
    KPK_RESULT = 'report.kpk_results'
    KPS_RESULT = 'report.kps_results'

    IN_ASSORTMENT = 'in_assortment_osa'
    IS_OOS = 'oos_osa'
    PSERVICE_CUSTOM_SCIF = 'pservice.custom_scene_item_facts'
    PRODUCT_FK = 'product_fk'
    SCENE_FK = 'scene_fk'
    EMPTY = 'Empty'
    DEFAULT = 'Default'
    TOP = 'Top'
    BOTTOM = 'Bottom'