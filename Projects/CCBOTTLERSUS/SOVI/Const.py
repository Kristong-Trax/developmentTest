
__author__ = 'Hunter'


class Const(object):
    UNITED = 'UNITED'
    OWN_MANUFACTURER_FK = 1  # for CCNA
    SSD_FK = 1
    STILL_FK = 2
    EXCLUDED_BRANDS = [
        "GENERAL OTHER",
    "GENERAL COFFEE OTHER",
    "GENERAL DAIRY OTHER",
    "GENERAL ENERGY OTHER",
    "GENERAL ISOTONIC OTHER",
    "GENERAL JC/DR SHELF STABLE OTHER",
    "GENERAL SSD OTHER",
    "GENERAL WATER OTHER",
    "Juice Other",
    "Tea Other"
    ]
    CATEGORY_FK = "category_fk"
    TEMPLATE_FK = "template_fk"
    MANUFACTURER_FK = "manufacturer_fk"
    BRAND_FK = "brand_fk"
    PRODUCT_FK = "product_fk"

    ATT4 = "att4"
    ATT4_VALUES = ['STILL', 'SSD']

    TOTAL = "SOVI - United Total Store"
    SSD = "SOVI - United Share of SSD Still"
    CATEGORY = "SOVI - United Share of Category"
    TEMPLATE_GROUP = "SOVI - United by Template Group"
    SHARE_TEMPLATE_GROUP = "SOVI - United Share of Template Group"
    SHARE_CATEGORY = "SOVI - United Share of Category"
    SHARE_CATEGORY_TEMPLATE_GROUP = "SOVI - United Category Share of Template Group"
    SHARE_SSD_TEMPLATE_GROUP = "SOVI - United SSD Still Share of Template Group"


    MANUFACTURER_TOTAL_STORE = "SOVI - Manufacturer Total Store"
    MANUFACTURER_SHARE_STORE = "SOVI - Manufacturer Share of Store"
    BRAND_SHARE_MANUFACTURER = "SOVI - Brand Share of Manufacturer"
    PRODUCT_SHARE_MANUFACTURER = "SOVI - Product Share of Brand"

    TEMPLATE_GROUP_MANUFACTURER = "SOVI - Manufacturer by Template Group"
    TEMPLATE_GROUP_STORE= "SOVI - Template Group Share of Store"
    MANUFACTURER_TEMPLATE_GROUP = "SOVI - Manufacturer Share of Template Group"
    BRAND_TEMPLATE_GROUP = "SOVI - Brand Share of Template Group"
    PRODUCT_TEMPLATE_GROUP = "SOVI - Product Share of Template Group"



    united_deliver_kpis =[]