import logging
import uuid
import json

from rome.core.utils import get_objects, current_milli_time
from rome.core.rows.tuples import sql_panda_building_tuples as join_building_tuples

FILE_LOGGER_ENABLED = False
try:
    FILE_LOGGER = logging.getLogger('rome_file_logger')
    HDLR = logging.FileHandler('/opt/logs/rome.log')
    FORMATTER = logging.Formatter('%(message)s')
    HDLR.setFormatter(FORMATTER)
    FILE_LOGGER.addHandler(HDLR)
    FILE_LOGGER.setLevel(logging.INFO)
    FILE_LOGGER.propagate = False
    FILE_LOGGER_ENABLED = True
except Exception:
    pass


def has_attribute(obj, key):
    """
    Check if a python object contains a given attributed or if a dict contains a key,
    via a common API
    :param obj: python object or dict that will be queried
    :param key: an attribute name or a key that will be looked
    :return: A boolean that describes if the attribute or key has been found
    """
    if type(obj) is dict:
        return key in obj
    else:
        return hasattr(obj, key)


def construct_rows(query_tree, entity_class_registry, request_uuid=None, filter_deleted=True):
    """
    This function constructs the rows that corresponds to the current orm.
    :param query_tree: a tree representation of the query
    :param entity_class_registry: class registry containing entity classes
    :param request_uuid: a facultative ID for the request
    :param filter_deleted: a boolean. When filter_deleted is True, matching objects that have
    been soft_deleted are filtered
    :return: a list of rows
    """

    # Find the SQLAlchemy model classes
    models = map(lambda x: entity_class_registry[x], query_tree.models)
    criteria = query_tree.where_clauses
    joining_criteria = query_tree.joining_clauses
    hints = []

    metadata = {}
    part1_start_time = current_milli_time()

    if request_uuid is None:
        request_uuid = uuid.uuid1()
    else:
        request_uuid = request_uuid

    columns = set([])
    rows = []

    model_set = models

    # Get the fields of the join result
    for selectable in model_set:
        selected_attributes = getattr(selectable, "_sa_class_manager", [])

        for field in selected_attributes:
            class_manager = None
            if has_attribute(selectable, "class_"):
                _class = getattr(selectable, "class_", None)
                class_manager = getattr(_class, "_sa_class_manager", None)
            elif has_attribute(selectable, "_sa_class_manager"):
                class_manager = getattr(selectable, "_sa_class_manager", None)
            if not class_manager:
                continue
            attribute = class_manager[field].__str__()
            if attribute is not None:
                columns.add(attribute)
    part2_start_time = current_milli_time()

    # Loading objects (from database)
    list_results = {}
    for selectable in model_set:
        table_name = selectable.__table__.name
        # authorized_secondary_indexes = get_attribute(selectable._model, "_secondary_indexes", [])
        authorized_secondary_indexes = []
        selected_hints = filter(lambda x: x.table_name == table_name and
                                (x.attribute == "id" or
                                 x.attribute in authorized_secondary_indexes),
                                hints)
        reduced_hints = map(lambda x: (x.attribute, x.value), selected_hints)
        objects = get_objects(table_name, hints=reduced_hints)
        # Filter soft_deleted objects
        if filter_deleted:
            objects = filter(lambda o: not ("deleted" in o and o["deleted"] == o["id"]), objects)
        list_results[table_name] = objects
    part3_start_time = current_milli_time()

    # Handling aliases
    for k in query_tree.aliases:
        list_results[k] = list_results[query_tree.aliases[k]]

    # Building tuples
    building_tuples = join_building_tuples
    tuples = building_tuples(query_tree,
                             list_results,
                             metadata=metadata)
    part4_start_time = current_milli_time()

    # Filtering tuples (cartesian product)
    for product in tuples:
        if len(product) > 0:
            rows += [product]
    part5_start_time = current_milli_time()

    # Reordering tuples (+ selecting attributes)
    part6_start_time = current_milli_time()

    if "sql" in metadata:
        sql_information = metadata["sql"]
    else:
        sql_information = {
            "models": str(models),
            "criteria": str(criteria),
            "joining_criteria": str(joining_criteria)
        }

    query_information = {
        "building_query": part2_start_time - part1_start_time,
        "loading_objects": part3_start_time - part2_start_time,
        "building_tuples": part4_start_time - part3_start_time,
        "filtering_tuples": part5_start_time - part4_start_time,
        "reordering_columns": part6_start_time - part5_start_time,
        "description": sql_information,
        "timestamp": current_milli_time(),
    }

    json_query_information = json.dumps(query_information)
    logging.debug(query_information)

    if FILE_LOGGER_ENABLED:
        FILE_LOGGER.info(json_query_information)

    return rows
