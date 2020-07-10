FK = 'fk'
KPI_NAME = 'kpi_name'
KPI_TYPE = 'kpi_type'
RESULT = 'result'
NUMERATOR_ID = 'numerator_id'
DENOMINATOR_ID = 'denominator_id'
NAME = 'name'
COMPONENT_KPI = 'component_kpi'
PARENT_KPI = 'parent_fkpi'
BRAND_FK = 'brand_fk'
BRAND_NAME = 'brand_name'
CATEGORY = 'category'  # not 'category_name'
PRODUCT_FK = 'product_fk'
MANUFACTURER_FK = 'manufacturer_fk'

DEN_FILTERS = 'den_filters'
NUM_FILTERS = 'num_filters'
DATASET_A = 'dataset_a'
DATASET_B = 'dataset_b'
TEST_A = 'test_a'
TEST_B = 'test_b'


UNITED_DELIVER = 'United Deliver'

# kpi types
AVAILABILITY = 'availability'
COOLER_PURITY = 'cooler_purity'
RESULTS_ANALYSIS = 'results_analysis'

# brands
COKE = 'COKE CLASSIC'
DIET_COKE = 'COKE DT'
COKE_ZERO = 'COKE ZERO'
SPRITE = 'SPRITE'


KPIs = {
    COMPONENT_KPI: [
        {
            NAME: 'Cooler Purity - Scene',
            KPI_TYPE: COOLER_PURITY,
            'minimum_threshold': 40,
            'purity_threshold': 95,
            DEN_FILTERS: {
                'template_name': ['Other Cooler'],
                UNITED_DELIVER: 'Y'
            },
            NUM_FILTERS: {MANUFACTURER_FK: 1}
        },
        {
            NAME: 'Required SSD Brands',
            KPI_TYPE: AVAILABILITY,
            DATASET_A: {
                BRAND_NAME: [COKE, DIET_COKE, COKE_ZERO, SPRITE]
            },
            DATASET_B: {
                'isin': {'att4': 'SSD', UNITED_DELIVER: 'Y'},
                'not isin': {BRAND_NAME: [COKE, DIET_COKE, COKE_ZERO, SPRITE]},
            },
            TEST_A: {BRAND_FK: 4},
            TEST_B: {BRAND_FK: 1}
        },
        {
            NAME: 'Required Tea SKUs',
            KPI_TYPE: AVAILABILITY,
            DATASET_A: {
                UNITED_DELIVER: 'Y',
                CATEGORY: 'Tea'
            },
            TEST_A: {PRODUCT_FK: 2}
        },
        {
            NAME: 'Required Water SKUs',
            KPI_TYPE: AVAILABILITY,
            DATASET_A: {
                UNITED_DELIVER: 'Y',
                CATEGORY: 'Water'
            },
            TEST_A: {PRODUCT_FK: 2}
        },
        {
            NAME: 'Required Energy SKUs',
            KPI_TYPE: AVAILABILITY,
            DATASET_A: {
                UNITED_DELIVER: 'Y',
                CATEGORY: 'Energy'
            },
            TEST_A: {PRODUCT_FK: 2}
        },
    ],
    PARENT_KPI: [
        {
            NAME: 'Cooler Purity',
            KPI_TYPE: RESULTS_ANALYSIS,
            COMPONENT_KPI: ['Cooler Purity - Scene']
        },
        {
            NAME: 'Contact Center',
            KPI_TYPE: RESULTS_ANALYSIS,
            COMPONENT_KPI: ['Required SSD Brands', 'Required Tea SKUs', 'Required Water SKUs', 'Required Energy SKUs']
        },
    ]
}
