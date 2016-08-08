__author__ = 'jonathan'

import logging
import time
import uuid
import traceback

from sqlalchemy.sql.expression import BinaryExpression
from sqlalchemy.util._collections import KeyedTuple

from lib.rome.core.dataformat import get_decoder
from lib.rome.core.lazy import LazyValue
from lib.rome.core.utils import get_objects, is_novabase

# from tuples import default_panda_building_tuples as join_building_tuples
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


def all_selectables_are_functions(models):
    return all(x._is_function for x in [y for y in models if not y.is_hidden])


def any_selectable_is_function(models):
    return any(x._is_function for x in [y for y in models if not y.is_hidden])


def has_attribute(obj, key):
    if type(obj) is dict:
        return key in obj
    else:
        return hasattr(obj, key)


def set_attribute(obj, key, value):
    if type(obj) is dict:
        obj[key] = value
    else:
        return setattr(obj, key, value)


def get_attribute(obj, key, default=None):
    if type(obj) is dict:
        return obj[key] if key in obj else default
    else:
        return getattr(obj, key, default)


def find_table_name(model):
    """This function return the name of the given model as a String. If the
    model cannot be identified, it returns "none".
    :param model: a model object candidate
    :return: the table name or "none" if the object cannot be identified
    """
    if has_attribute(model, "__tablename__"):
        return model.__tablename__
    if has_attribute(model, "table"):
        return model.table.name
    if has_attribute(model, "class_"):
        return model.class_.__tablename__
    if has_attribute(model, "clauses"):
        for clause in model.clauses:
            return find_table_name(clause)
    return "none"


def extract_models(l):
    already_processed = set()
    result = []
    for selectable in [x for x in l if not x._is_function]:
        if not selectable._model in already_processed:
            already_processed.add(selectable._model)
            result += [selectable]
    return result


def extract_sub_row(row, selectables, labels, default_tablename):
    """Adapt a row result to the expectation of sqlalchemy.
    :param row: a list of python objects
    :param selectables: a list entity class
    :return: the response follows what is required by sqlalchemy (if len(model)==1, a single object is fine, in
    the other case, a KeyTuple where each sub object is associated with it's entity name
    """
    if len(selectables) > 1:
        product = []
        value = {}
        for x in zip(labels, row):
            label = x[0]
            v = x[1]
            product = product + [v]
            value[label] = v
        return value
    else:
        return row[0]


def intersect(b1, b2):
    return [val for val in b1 if val in b2]


def flatten(lis):
    """Given a list, possibly nested to any level, return it flattened."""
    new_lis = []
    for item in lis:
        if type(item) == type([]):
            new_lis.extend(flatten(item))
        else:
            new_lis.append(item)
    return new_lis


def extract_table_data(term):
    term_value = str(term)
    if "." in term_value:
        return {"table": term_value.split(".")[0], "column": term_value.split(".")[1]}
    else:
        return None


def extract_joining_criterion(exp):
    from lib.rome.core.expression.expression import BooleanExpression
    if type(exp) is BooleanExpression:
        return map(lambda x:extract_joining_criterion(x), exp.exps)
    elif type(exp) is BinaryExpression:
        return [[extract_table_data(exp.left)] + [extract_table_data(exp.right)]]
    else:
        return []


def extract_joining_criterion_from_relationship(rel, local_table):
    local_tabledata = {"table": local_table, "column": rel.local_fk_field}
    remote_tabledata = {"table": rel.remote_object_tablename, "column": rel.remote_object_field}
    return [local_tabledata, remote_tabledata]


def wrap_with_lazy_value(value, only_if_necessary=True, request_uuid=None):
    if value is None:
        return None
    if only_if_necessary and type(value).__name__ in ["int", "str", "float", "unicode"]:
        return value
    elif type(value) is dict and "timezone" in value:
        decoder = get_decoder(request_uuid=request_uuid)
        return decoder.desimplify(value)
    else:
        return LazyValue(value, request_uuid)


def construct_rows(models, criterions, hints, session=None, request_uuid=None, order_by=None):

    """This function constructs the rows that corresponds to the current orm.
    :return: a list of row, according to sqlalchemy expectation
    """

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

    model_set = extract_models(models)

    """ Get the fields of the join result """
    for selectable in model_set:
        labels += [find_table_name(selectable._model)]
        if selectable._attributes == "*":
            try:
                selected_attributes = selectable._model._sa_class_manager
            except:
                traceback.print_exc()
                selected_attributes = selectable._model.class_._sa_class_manager
                pass
        else:
            selected_attributes = [selectable._attributes]

        for field in selected_attributes:
            attribute = None
            if has_attribute(models, "class_"):
                attribute = selectable._model.class_._sa_class_manager[field].__str__()
            elif has_attribute(models, "_sa_class_manager"):
                attribute = selectable._model._sa_class_manager[field].__str__()
            if attribute is not None:
                columns.add(attribute)
    part2_starttime = current_milli_time()

    """ Loading objects (from database) """
    list_results = []
    for selectable in model_set:
        tablename = find_table_name(selectable._model)
        authorized_secondary_indexes = get_attribute(selectable._model, "_secondary_indexes", [])
        selected_hints = filter(lambda x: x.table_name == tablename and (x.attribute == "id" or x.attribute in authorized_secondary_indexes), hints)
        reduced_hints = map(lambda x:(x.attribute, x.value), selected_hints)
        objects = get_objects(tablename, request_uuid=request_uuid, skip_loading=False, hints=reduced_hints)
        list_results += [objects]
    part3_starttime = current_milli_time()

    """ Building tuples """
    building_tuples = join_building_tuples
    tuples = building_tuples(list_results, labels, criterions, hints, metadata=metadata, order_by=order_by)
    part4_starttime = current_milli_time()

    """ Filtering tuples (cartesian product) """
    keytuple_labels = None
    for product in tuples:
        if len(product) > 0:
            if keytuple_labels is None:
                keytuple_labels = map(lambda e: e["_nova_classname"], product)
            row = product
            rows += [extract_sub_row(row, model_set, labels, tablename)]
    part5_starttime = current_milli_time()
    decoder = get_decoder(request_uuid=request_uuid)

    """ Reordering tuples (+ selecting attributes) """
    showable_selection = [x for x in models if (not x.is_hidden) or x._is_function]
    part6_starttime = current_milli_time()

    """ Selecting attributes """
    if any_selectable_is_function(models):
        final_row = []
        for selection in showable_selection:
            if selection._is_function:
                value = selection._function._function(rows)
                final_row += [value]
            else:
                final_row += [None]
        final_row = map(lambda x: decoder.desimplify(x), final_row)
        return [final_row]
    else:
        first_row = rows[0] if len(rows) > 0 else {}
        first_row_is_novabase = is_novabase(first_row)
        first_row_has_tablename_attribute = has_attribute(first_row, tablename)
        columns_oriented_values = []
        for selection in showable_selection:
            def generate_row_operation(selection, tablename, request_uuid):
                if selection._is_function:
                    return selection._function._function
                else:
                    key = tablename
                    if not first_row_is_novabase and first_row_has_tablename_attribute:
                        return lambda r: wrap_with_lazy_value(get_attribute(r, key), request_uuid)
                    else:
                        if selection._attributes != "*":
                            return lambda r: wrap_with_lazy_value(get_attribute(r, selection._attributes), request_uuid)
                        else:
                            return lambda r: wrap_with_lazy_value(r, request_uuid)
            row_operation = generate_row_operation(selection, tablename, request_uuid)
            columns_oriented_values += [map(row_operation, rows)]
    final_rows = list(map(list, zip(*columns_oriented_values)))
    if len(showable_selection) == 1:
        final_rows = map(lambda x: x[0], final_rows)
    part7_starttime = current_milli_time()

    query_information = """{"building_query": %s, "loading_objects": %s, "building_tuples": %s, "filtering_tuples": %s, "reordering_columns": %s, "selecting_attributes": %s, "description": "%s", "timestamp": %i}""" % (
        part2_starttime - part1_starttime,
        part3_starttime - part2_starttime,
        part4_starttime - part3_starttime,
        part5_starttime - part4_starttime,
        part6_starttime - part5_starttime,
        part7_starttime - part6_starttime,
        metadata["sql"] if "sql" in metadata else """{\\"models\\": \\"%s\\", \\"criterions\\": \\"%s\\"}""" % (models, criterions),
        current_milli_time()
    )

    logging.info(query_information)
    if file_logger_enabled:
        file_logger.info(query_information)

    return final_rows
