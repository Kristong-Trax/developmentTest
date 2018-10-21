import os
__author__ = 'Elyashiv'


class Const(object):
    MANUFACTURER_FK = 1  # for CCNA
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

    TOTAL = "SOVI_Total_Store"
    TEMPLATE = "SOVI_Template_Store"
    ATT4 = "SOVI_Att4"
    CATEGORY = "SOVI_Category"
    MANUFACTURER = "SOVI_Manufacturer"
    BRAND = "SOVI_Brand"
    PRODUCT = "SOVI_Product_Name"
