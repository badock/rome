import logging
import time
import uuid

from rome.core.utils import get_objects
from tuples import sql_panda_building_tuples as join_building_tuples

file_logger_enabled = False
try:
    file_logger = logging.getLogger('rome_file_logger')
    hdlr = logging.FileHandler('/opt/logs/rome.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    file_logger.addHandler(hdlr)
    file_logger.setLevel(logging.INFO)
    file_logger_enabled = True
except:
    pass


def has_attribute(obj, key):
    if type(obj) is dict:
        return key in obj
    else:
        return hasattr(obj, key)


def construct_rows(query_tree, entity_class_registry, request_uuid=None):

    """This function constructs the rows that corresponds to the current orm.
    :return: a list of row, according to sqlalchemy expectation
    """

    # Find the SQLAlchemy model classes
    models = map(lambda x: entity_class_registry[x], query_tree.models)
    criteria = query_tree.where_clauses
    joining_criteria = query_tree.joining_clauses
    hints = []
    order_by = None

    current_milli_time = lambda: int(round(time.time() * 1000))

    metadata = {}
    part1_starttime = current_milli_time()

    if request_uuid is None:
        request_uuid = uuid.uuid1()
    else:
        request_uuid = request_uuid

    labels = []
    columns = set([])
    rows = []

    # model_set = extract_models(models)
    model_set = models

    """ Get the fields of the join result """
    for selectable in model_set:
        labels += [selectable.__table__.name]
        selected_attributes = selectable._sa_class_manager

        for field in selected_attributes:
            attribute = None
            if has_attribute(selectable, "class_"):
                attribute = selectable.class_._sa_class_manager[field].__str__()
            elif has_attribute(selectable, "_sa_class_manager"):
                attribute = selectable._sa_class_manager[field].__str__()
            if attribute is not None:
                columns.add(attribute)
    part2_starttime = current_milli_time()

    """ Loading objects (from database) """
    list_results = {}
    for selectable in model_set:
        tablename = selectable.__table__.name
        # authorized_secondary_indexes = get_attribute(selectable._model, "_secondary_indexes", [])
        authorized_secondary_indexes = []
        selected_hints = filter(lambda x: x.table_name == tablename and (x.attribute == "id" or x.attribute in authorized_secondary_indexes), hints)
        reduced_hints = map(lambda x:(x.attribute, x.value), selected_hints)
        objects = get_objects(tablename, request_uuid=request_uuid, skip_loading=False, hints=reduced_hints)
        list_results[tablename] = objects
    part3_starttime = current_milli_time()

    # Handling aliases
    for k in query_tree.aliases:
        list_results[k] = list_results[query_tree.aliases[k]]

    """ Building tuples """
    building_tuples = join_building_tuples
    tuples = building_tuples(query_tree, list_results, metadata=metadata, order_by=order_by)
    part4_starttime = current_milli_time()

    """ Filtering tuples (cartesian product) """
    # keytuple_labels = None
    for product in tuples:
        if len(product) > 0:
            rows += [product]
    part5_starttime = current_milli_time()

    """ Reordering tuples (+ selecting attributes) """
    part6_starttime = current_milli_time()

    query_information = """{"building_query": %s, "loading_objects": %s, "building_tuples": %s, "filtering_tuples": %s, "reordering_columns": %s, "description": "%s", "timestamp": %i}""" % (
        part2_starttime - part1_starttime,
        part3_starttime - part2_starttime,
        part4_starttime - part3_starttime,
        part5_starttime - part4_starttime,
        part6_starttime - part5_starttime,
        metadata["sql"] if "sql" in metadata else """{\\"models\\": \\"%s\\", \\"criteria\\": \\"%s\\", \\"joining_criteria\\": \\"%s\\"}""" % (models, criteria, joining_criteria),
        current_milli_time()
    )

    logging.info(query_information)
    if file_logger_enabled:
        file_logger.info(query_information)

    return rows
