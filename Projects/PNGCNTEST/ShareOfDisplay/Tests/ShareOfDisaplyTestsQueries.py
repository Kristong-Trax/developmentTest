
__author__ = 'Dudi S'


def get_display_surface_by_scene_query(scene_fk):
    return """select scene_fk, display_fk, surface
            from probedata.display_surface where scene_fk = {};""".format(scene_fk)


def get_display_surface_by_session_query(session_uid):
    return """select scene_fk, display_fk, surface
                from probedata.display_surface ds
                join probedata.scene sc on sc.pk = ds.scene_fk where session_uid = '{}';""".format(session_uid)


def get_display_item_facts_by_scene_query(scene_fk):
    return """select df.*
                from probedata.display_surface ds
                 join report.display_item_facts df
                  on df.display_surface_fk = ds.pk where scene_fk = {};""".format(scene_fk)


def get_display_item_facts_by_session_query(session_uid):
    return """select df.*
                    from probedata.display_surface ds
                     join report.display_item_facts df
                      on df.display_surface_fk = ds.pk
                     join probedata.scene sc on sc.pk = ds.scene_fk where session_uid = '{}';""".format(session_uid)