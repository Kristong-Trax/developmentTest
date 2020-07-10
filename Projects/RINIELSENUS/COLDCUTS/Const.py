class Consts(object):
    SHELF_MAP = 'shelf_map'
    SOS = 'Facings SOS'
    SCENE_LOCATION = 'Scene Location'
    HORIZONTAL_SHELF_POSITION = 'Horizontal Shelf Position'
    VERTICAL_SHELF_POSITION = 'Vertical Shelf Position'
    BLOCKING = 'Blocking'
    BAY_POSITION = 'Bay Position'
    BLOCKING_ORIENTATION = 'Block Orientation'
    KPI_TYPE = 'kpi_type'
    KPI_NAME = 'kpi_client_name'
    FINAL_FACINGS = 'final_facings'
    DISTRIBUTION = 'Distribution'
    KPI_TYPE = 'kpi_type'
    ACTUAL_TYPE = 'KPI Type'
    # SCENE_FK = 'scene_fk
    BLOCK_ADJ = 'Block Adjacency'

    CUSTOM_RESULT = {"Yes": 32,
                     "No": 31,
                     "Right": 29,
                     "Left": 27,
                     "Center": 28,
                     "Blocked": 37,
                     "Not Blocked": 38,
                     "Vertical": 39,
                     "Horizontal": 40,
                     "Top": 33,
                     "Middle": 35,
                     "Eye": 34,
                     "Bottom": 36
                     }

    shelf_map = {
        "1": {
            "1": "Bottom"
        },
        "2": {
            "1": "Bottom",
            "2": "Eye",
        },
        "3": {
            "1": "Bottom",
            "2": "Middle",
            "3": "Eye",
        },
        "4": {
            "1": "Bottom",
            "2": "Middle",
            "3": "Eye",
            "4": "Top",
        },
        "5": {
            "1": "Bottom",
            "2": "Middle",
            "3": "Eye",
            "4": "Eye",
            "5": "Top",
        },
        "6": {
            "1": "Bottom",
            "2": "Middle",
            "3": "Middle",
            "4": "Eye",
            "5": "Eye",
            "6": "Top",
        },
        "7": {
            "1": "Bottom",
            "2": "Middle",
            "3": "Middle",
            "4": "Eye",
            "5": "Eye",
            "6": "Eye",
            "7": "Top",
        },
        "8": {
            "1": "Bottom",
            "2": "Middle",
            "3": "Middle",
            "4": "Middle",
            "5": "Eye",
            "6": "Eye",
            "7": "Eye",
            "8": "Top",
        },
        "9": {
            "1": "Bottom",
            "2": "Bottom",
            "3": "Middle",
            "4": "Middle",
            "5": "Middle",
            "6": "Eye",
            "7": "Eye",
            "8": "Eye",
            "9": "Top",
        },
        "10": {
            "1": "Bottom",
            "2": "Bottom",
            "3": "Middle",
            "4": "Middle",
            "5": "Middle",
            "6": "Eye",
            "7": "Eye",
            "8": "Eye",
            "9": "Top",
            "10": "Top",
        },
        "11": {
            "1": "Bottom",
            "2": "Bottom",
            "3": "Middle",
            "4": "Middle",
            "5": "Middle",
            "6": "Eye",
            "7": "Eye",
            "8": "Eye",
            "9": "Top",
            "10": "Top",
            "11": "Top",
        },
    }
