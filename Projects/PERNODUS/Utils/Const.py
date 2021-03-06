#!/usr/bin/python
# -*- coding: utf-8 -*-

_author_ = 'NicolasKeeton'

class Const(object):


    #sheets
    # PRESENCE, ADJACENCY, BLOCKING, ANCHOR, BASE_SPACE, EYE_LEVEL = 'Presence','Adjacency', 'Blocking', 'Anchor',
    # 'Base Measurement','Eye Level'



    # SHEETS_MAIN= { PRESENCE, ADJACENCY, BLOCKING, ANCHOR, BASE_SPACE, EYE_LEVEL}


    #generic columns
    KPI_NAME = "KPI NAME"



    #column of param and vlalues:
    SOVI_NAME = "KPI NAME"
    KPI_RULE = "KPI RULE"
    PARAM_TYPE = "param"
    PARAM_VALUES = "value"
    list_value = "list"
    PRODUCT_ATTRIBUTE = "Product Att"



    #DISPLAY KPIS
    Count_of_display = 'COUNT_OF_DISPLAY'
    Brand_on_display = 'BRANDS_ON_DISPLAY'
    SOS_on_display_manufacturer  = 'SOS_ON_DISPLAY_MANUFACTURER'
    SOS_on_display_brand  = 'SOS_ON_DISPLAY_BRANDS'
    Share_of_display_manufacturer = 'SHARE_OF_DISPLAY_MANUFACTURER'
    Share_of_display_brand = 'SHARE_OF_DISPLAY_BRANDS'
    Solo_Shared_display = 'SOLO_SHARED'



    #columns
    brand = 'brand_name'
    brand_fk = 'brand_fk'
    manufacturer = 'manufacturer_name'
    manufacturer_fk = 'manufacturer_fk'
    category = 'category'
    category_fk = 'category_fk'
    sub_category = 'sub_category'
    sub_category_fk = 'sub_category_fk'
    facings = 'facings'
    template_name = 'template_name'
    scene_id = 'scene_id'
    template_fk = 'template_fk'


    #
    product_type = 'product_type'
    SKU = 'SKU'
    OTHER = 'Other'
    facings = 'facings'
