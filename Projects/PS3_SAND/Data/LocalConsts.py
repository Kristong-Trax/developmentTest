__author__ = 'Elyashiv'


class Consts(object):

    NOT_BRAND = {"General", "General."}
    GOOGLE_BRAND = "Google Play"
    APPLE_BRAND = "Apple"
    GOOGLE = "Google"
    EXCLUDE_TEMPLATE_GROUP = ["Competitor Fixture", "Free Peg", "New Fixture"]

    ENTRY = "Entry"
    EXIT = "Exit"

# external targets of new markets

    TARGETS_OPERATION = "Fixture Targets"

    FIXTURES_TARGET = "fixtures_target"
    TEMPLATE_GROUP = "template_group"
    TEMPLATE_FK = "template_fk"
    AUTOMATIC_ADDED = "automatic_added"
    SESSION_OF_NEW_TARGET = "new_target_session"
    STORE_NUMBER_1 = "store_number_1"

    EXIT_TEMPLATE_FK = "exit_template_fk"
    ENTRY_TEMPLATE_FK = "entry_template_fk"

    KEY_JSON_FIELDS = [STORE_NUMBER_1, TEMPLATE_GROUP]
    DATA_JSON_FIELDS = [FIXTURES_TARGET, AUTOMATIC_ADDED, SESSION_OF_NEW_TARGET]

# fields of eye level template

    MIN_SHELVES = "num. of shelves min"
    MAX_SHELVES = "num. of shelves max"
    IGNORE_FROM_TOP = "num. ignored from top"
    IGNORE_FROM_BOTTOM = "num. ignored from bottom"

# fields of required_fixtures

    TEMPLATE_NAMES = "template_names"
    TEMPLATE_FKS = "template_fks"
    SCENE_FKS = "scene_fks"
    REQUIRED_AMOUNT = "required_amount"
    FIXTURE_FK = "fixture_fk"

# levels for next dict

    STORE_LEVEL = "store"
    VISIT_LEVEL = "visit"
    TEMPLATE_LEVEL = "template"
    FIXTURE_LEVEL = "fixture"

# KPI names for next dict

    FACINGS_COMPLIANCE = "FC"
    POG_COMPLIANCE = "POG"
    DENOM_AVAILABILITY = "DA"
    SHARE_OF_COMPETITOR = "SOC"
    FIXTURE_AVAILABILITY = "FA"

# dict with all the levels' names of the main KPIs

    KPIS_DICT = {
        POG_COMPLIANCE: {
            VISIT_LEVEL: "POG Compliance - Visit",
            STORE_LEVEL: {ENTRY: "POG Compliance - Store Entry", EXIT: "POG Compliance - Store Exit"},
            TEMPLATE_LEVEL: {ENTRY: "POG Compliance - Template Entry", EXIT: "POG Compliance - Template Exit"},
            FIXTURE_LEVEL: "POG Compliance - Fixture"
            # FIXTURE_LEVEL: "POG Compliances - Fixture"
        },
        DENOM_AVAILABILITY: {
            VISIT_LEVEL: "Denom Availability - Visit",
            STORE_LEVEL: {ENTRY: "Denom Availability - Store Entry", EXIT: "Denom Availability - Store Exit"},
            TEMPLATE_LEVEL: {ENTRY: "Denom Availability - Template Entry", EXIT: "Denom Availability - Template Exit"},
            FIXTURE_LEVEL: "Denom Availability - Fixture"
        },
        FIXTURE_AVAILABILITY: {
            STORE_LEVEL: "Fixture Availability - Store",
            TEMPLATE_LEVEL: "Fixture Availability - Template",
        },
        FACINGS_COMPLIANCE: {
            STORE_LEVEL: {ENTRY: "Facings Compliance - Store Entry", EXIT: "Facings Compliance - Store Exit"},
            TEMPLATE_LEVEL: {ENTRY: "Facings Compliance - Template Entry", EXIT: "Facings Compliance - Template Exit"},
            FIXTURE_LEVEL: "Facing Compliance"
        },
        SHARE_OF_COMPETITOR: {
            STORE_LEVEL: {ENTRY: "Share of Competitor - Store Entry", EXIT: "Share of Competitor - Store Exit"},
            FIXTURE_LEVEL: "SOS BRAND out of GOOGLE in SCENE"
        },
    }

# Other KPIs:

    SOS_SCENE = "SOS BRAND out of SCENE"
    SOS_TARGET_ROG = "SOS TARGET - realogram"
    SOS_TARGET_POG = "SOS TARGET - planogram"

    CHOSEN_SCENES_EXIT = "chosen_scenes_exit"
    CHOSEN_SCENES_ENTRY = "chosen_scenes_entry"
    PASSED_RULE = "adding_rule_passed"
    ADDED_EXTERNAL_TARGETS = "added_external_targets"

    FIXTURE_POSITION_ROG = "FIXTURE POSITION - realogram"
    FIXTURE_POSITION_POG = "FIXTURE POSITION - planogram"

    POP_COMPLIANCE = "POP Compliance"

    POG_STATUS = "POG STATUS"
    POG_IN_POS = "IN POSITION"
    POG_NOT_IN_POS = "NOT IN POSITION"
    POG_EXTRA = "EXTRA"
    POG_MISSING = "MISSINGS"
    POG_PRODUCT = "POG PRODUCT"
    MISSING_DENOMINATIONS = "missing_denominations"
    MISSING_DENOMINATIONS_STORE = "Missing Denominations - Store Exit"
    MAPPING_STATUS_DICT = {1: POG_EXTRA, 2: POG_NOT_IN_POS, 3: POG_IN_POS, 4: POG_MISSING}

# Anomalies of POG:

    NO_ANOMALY = "no anomaly"
    DIFFERENT_PRODUCT_POG_ANOMALY = "different number of products in the POGs"
    DIFFERENT_PRODUCT_SCENE_ANOMALY = "different number of products in the scenes"
    SCENES_WITHOUT_POG = "at least one of the scenes does not have a POG"
    NO_SCENE_WITH_SCENE_TYPE = "the session does not have the required scene type"
    DIFFERENT_NUMBER_OF_ENTRY_AND_EXIT_SCENES = "different number of entry and exit scenes"

# messages:

    MSG_TEMPLATE_GROUP_DELETED = "Automatic target deleted {} since the template group has deleted"
    MSG_HIGHER_TARGET = "Automatic target deleted {} since the template group needs a higher target"
    MSG_NO_FIXTURE_TARGETS_FOR_DATE = "There is no fixture targets for this visit_date."
    MSG_NO_FIXTURE_TARGETS_FOR_STORE = "There is no fixture targets for store_number_1 '{}'."
    MSG_COMMIT_EXTERNAL_TARGETS = "Committing new external targets"

    UPDATE_EXTERNAL_TARGET_QUERY = "UPDATE static.kpi_external_targets SET end_date='{}' WHERE pk in ({});"
