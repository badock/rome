import datetime
import re
import traceback

import pandas as pd

from rome.core.utils import DATE_FORMAT, datetime_to_int


def correct_boolean_int(expression_str):
    """
    Replace 'deleted == x' where x in [0, None] by 'deleted != 1'.
    :param expression_str: a string expression containing boolean conditions
    :return: a modified string expression
    """
    expression_str = expression_str.replace("___deleted == 0", "___deleted != 1")
    return expression_str


def correct_expression(expression_str):
    """
    Replace 'x is None' by 'x != x' and 'x is not None' by 'x == x'. This trick
    is inspired by a comment found on:
    http://stackoverflow.com/questions/26535563/querying-for-nan-and-other-names-in-pandas .
    :param expression_str: expression_str: a string expression containing boolean conditions
    :return: a modified string expression
    """
    terms = expression_str.split()
    clean_expression = " ".join(terms)
    if len(terms) > 0:
        variable_name = terms[0]
        variable_name = variable_name.replace("(", "")
        if "is not None" in clean_expression:
            clean_expression = "(%s == %s)" % (variable_name,
                                               variable_name)
        if "is None" in clean_expression:
            clean_expression = "(%s != %s)" % (variable_name,
                                               variable_name)
    return clean_expression


def drop_y(data_frame):
    """
    Drop duplicate columns that appeared after a merge of two pandas data frames.
    Usually these columns ends with "_y".
    :param data_frame: a pandas data frame
    :return: a reference to the data frame
    """
    to_drop = [x for x in data_frame if x.endswith('_y')]
    result = data_frame.drop(to_drop, axis=1, inplace=True)
    return result


def rename_x(data_frame):
    """
    Rename duplicate columns that appeared after a merge of two pandas data frames.
    Usually these columns ends with "_x"
    :param data_frame: a pandas data frame
    :return: a reference to the data frame
    """
    cols = list(data_frame.columns)
    fixed_columns = map(lambda x: re.sub("_x$", "", x), cols)
    data_frame.columns = fixed_columns
    return data_frame


def extract_joining_pairs(criterion):
    """
    Extract pairs of attributes that must be used to decide which columns
    should be used to make the join. For instance, a criterion such as
    '"Authors".id = "Books".author_id' will become ["Authors.id", "Books.id"].
    :param criterion: a string expression
    :return: a list where each item is a list that contains 2 attribute's names
    """
    word_pattern = "[_a-zA-Z0-9]+"
    for x in ["\"%s\".%s[ ]*=[ ]*\"%s\".%s", "%s.%s[ ]*=[ ]*%s.%s"]:
        joining_criterion_pattern = x % (
            word_pattern, word_pattern, word_pattern, word_pattern
        )
        matches = re.search(joining_criterion_pattern, criterion)
        if matches is not None:
            joining_pair = criterion.split("=")
            joining_pair = map(lambda x: x.strip().replace("\"", ""), joining_pair)
            joining_pair = sorted(joining_pair)
            return [joining_pair]
    return []


def date_value_to_int(local_value):
    """
    Transform a date value (dict, or datatime value) to an int value
    :param local_value: a data value
    :return: datetime as an int value
    """
    date_value = None
    if isinstance(local_value, dict) and u"value" in local_value:
        date_value = str(local_value[u"value"])
    if isinstance(local_value, dict) and "value" in local_value:
        date_value = local_value["value"]
    if date_value is not None:
        date_object = datetime.datetime.strptime(date_value,
                                                 DATE_FORMAT)
        return datetime_to_int(date_object)
    return local_value


def sql_panda_building_tuples(query_tree,
                              lists_results,
                              metadata=None,
                              subqueries_variables=None):
    """
    Build tuples (join operator in relational algebra).
    :param query_tree: a tree representation of the query
    :param lists_results: a dict containing a list of objects corresponding
    to each entity used in the query.
    :param metadata: a dict that contains metadata that will be used to
    analyse how data have been joined.
    :param subqueries_variables: a dict that contains variables whose values
    have been set in sub queries.
    :return: a list of rows
    """

    if not subqueries_variables:
        subqueries_variables = {}

    labels = lists_results.keys()
    if metadata is None:
        metadata = {}

    # Initializing data indexes.
    table_id_index = {}
    table_index = {}
    for label in labels:
        table_id_index[label] = {}
        table_index[label] = 1
    for (label, k) in zip(labels, lists_results):
        for result in lists_results[k]:
            id_ = result["id"]
            table_id_index[label][id_] = result

    # Collecting dependencies.
    joining_pairs = []
    non_joining_criteria = []
    _joining_pairs_str_index = {}
    needed_columns = {}

    adapted_non_pandas_criteria = []
    adapted_pandas_criteria = []
    for criterion in query_tree.where_clauses:
        adapted_criterion = criterion
        adapted_criterion = re.sub("\\\'", "\"", adapted_criterion)
        adapted_criterion = re.sub(" = ", " == ", adapted_criterion)
        adapted_criterion = re.sub("AND", " and ", adapted_criterion)
        adapted_criterion = re.sub("OR", " or ", adapted_criterion)
        adapted_criterion = re.sub("IN", " in ", adapted_criterion)
        adapted_panda_criterion = adapted_criterion
        adapted_nonpanda_criterion = adapted_criterion
        for label in labels:
            patterns = ["\"+%s\"+." % (label)]
            for x in patterns:
                adapted_panda_criterion = re.sub(x, "%s__" % (label), adapted_panda_criterion)
                adapted_nonpanda_criterion = re.sub(x, "%s." % (label), adapted_nonpanda_criterion)
        adapted_pandas_criteria += [adapted_panda_criterion]
        adapted_non_pandas_criteria += [adapted_nonpanda_criterion]

    for criterion in query_tree.joining_clauses:
        _joining_pairs = extract_joining_pairs(criterion)

        if len(_joining_pairs) > 0:
            _joining_pairs_str = str(sorted(_joining_pairs[0]))
            if not _joining_pairs_str in _joining_pairs_str_index:
                _joining_pairs_str_index[_joining_pairs_str] = 1
                joining_pairs += _joining_pairs

    # Cloning lists_results.
    for label in labels:
        needed_columns[label] = ["id"]
    for criterion in adapted_non_pandas_criteria:
        word_pattern = "[_a-z_A-Z][_a-z_A-Z0-9]*"
        property_pattern = r"%s\.%s" % (word_pattern, word_pattern)
        for match in re.findall(property_pattern, str(criterion)):
            table = match.split(".")[0]
            attribute = match.split(".")[1]
            if table in needed_columns and attribute not in needed_columns[table]:
                needed_columns[table] += [attribute]
    for pair in joining_pairs:
        for each in pair:
            table = each.split(".")[0]
            attribute = each.split(".")[1]
            if table in needed_columns and attribute not in needed_columns[table]:
                needed_columns[table] += [attribute]

    # Preparing the query for pandasql.
    attribute_clause = ",".join(map(lambda x: "%s.id" % (x), labels))
    from_clause = " join ".join(labels)
    where_join_clause = " and ".join(map(lambda x: "%s == %s" % (x[0], x[1]),
                                         joining_pairs))
    where_criterions_clause = " and ".join(map(lambda x: str(x),
                                               adapted_pandas_criteria))

    where_clause = "1==1 "
    if where_join_clause != "":
        where_clause += " and %s" % (where_join_clause)
    if where_criterions_clause != "":
        where_clause += " and %s" % (where_criterions_clause)

    # Update where clause with variables collected in sub queries
    for (variable_name, value) in subqueries_variables.iteritems():
        if type(value) is list:
            str_value = "(%s)" % (",".join(map(lambda x: "%s" % (x), value)))
        else:
            str_value = "%s"
        where_join_clause = where_join_clause.replace(variable_name, str_value)
        where_criterions_clause = where_criterions_clause.replace(variable_name, str_value)
        where_clause = where_clause.replace(variable_name, str_value)

    sql_query = "SELECT %s FROM %s WHERE %s" % (attribute_clause, from_clause,
                                                where_clause)
    metadata["sql"] = sql_query

    # Preparing Dataframes.
    env = {}
    for (label, k) in zip(labels, lists_results):
        current_dataframe_all = pd.DataFrame(data=lists_results[k])
        if len(current_dataframe_all.columns) > 0:
            dataframe = current_dataframe_all[needed_columns[label]]
        else:
            if len(query_tree.outer_join_models) > 0:
                empty_columns = {}
                for column in needed_columns[label]:
                    empty_columns[column] = []
                dataframe = pd.DataFrame(empty_columns)
            else:
                return []
        dataframe.columns = map(lambda c: "%s__%s" % (label, c),
                                needed_columns[label])
        env[label] = dataframe

    # Construct the resulting rows.
    if len(labels) > 1 and len(filter(lambda x: len(x) == 0, lists_results)) > 0:
        return []

    result = None

    if len(lists_results) > 1:
        processed_tables = []

        if len(joining_pairs) > 0:
            # Do a join between the tables
            for joining_pair in joining_pairs:
                # Preparing the tables that will be joined.
                attribute_1 = joining_pair[0].strip()
                attribute_2 = joining_pair[1].strip()
                tablename_1 = attribute_1.split(".")[0]
                tablename_2 = attribute_2.split(".")[0]

                if tablename_1 not in env or tablename_2 not in env:
                    return []
                dataframe_1 = env[tablename_1] if not tablename_1 in processed_tables else result
                dataframe_2 = env[tablename_2] if not tablename_2 in processed_tables else result

                refactored_attribute_1 = attribute_1.split(
                    ".")[0] + "__" + attribute_1.split(".")[1]
                refactored_attribute_2 = attribute_2.split(
                    ".")[0] + "__" + attribute_2.split(".")[1]
                # Join the tables.
                try:
                    merge_method = "outer"
                    if len(query_tree.outer_join_models) > 0:
                        if tablename_1 in query_tree.outer_join_models:
                            merge_method = "left"
                    result = pd.merge(dataframe_1, dataframe_2,
                                      left_on=refactored_attribute_1,
                                      right_on=refactored_attribute_2,
                                      how=merge_method)
                    drop_y(result)
                    rename_x(result)
                except KeyError:
                    return []
                # Update the history of processed tables.
                processed_tables += [tablename_1, tablename_2]
                processed_tables = list(set(processed_tables))
        elif len(table_index.keys()) >= 2:
            # Do a cartesian product manually
            ids_array = []
            for tablename in table_index.keys():
                old_ids_array = ids_array
                ids_array = []
                if tablename not in env:
                    continue
                if len(old_ids_array) == 0:
                    ids_array = map(
                        lambda x: {"%s__id" % (tablename): x["id"]},
                        lists_results[tablename])
                else:
                    for element in lists_results[tablename]:
                        for row in old_ids_array:
                            row_copy = row.copy()
                            row_copy["%s__id" % (tablename)] = element["id"]
                            ids_array += [row_copy]
                processed_tables += [tablename]
            result = pd.DataFrame(ids_array)

    # Fixing none result.
    if result is None:
        if len(labels) == 0:
            return []
        result = env[labels[0]]

    # Update where clause.
    new_where_clause = where_clause
    new_where_clause = " ".join(new_where_clause.split())
    # new_where_clause = new_where_clause.replace("()", "1==1")
    new_where_clause = new_where_clause.replace("1==1 and", "")
    new_where_clause = new_where_clause.replace("is None", "== 0")
    new_where_clause = new_where_clause.replace("is not None", "!= 0")
    new_where_clause = new_where_clause.replace("IS NULL", "== 0")
    new_where_clause = new_where_clause.replace("IS NOT NULL", "!= 0")
    new_where_clause = new_where_clause.replace("NOT", " not ")
    new_where_clause = new_where_clause.strip()

    # <Quick fix for handling dates>
    fix_date = True
    if fix_date:
        for column_name in result:
            column = result[column_name]
            if column.dtype.name == "object" and len(column) > 0:
                column_item = column[0]
                if type(column_item) is not dict:
                    continue
                if "simplify_strategy" in column_item and column_item["simplify_strategy"] == "datetime":
                    result[column_name] = column.apply(lambda x: x["value"])
    # </Quick fix for handling dates>

    for table in needed_columns:
        for attribute in needed_columns[table]:
            old_pattern = "%s.%s" % (table, attribute)
            new_pattern = "%s__%s" % (table, attribute)
            new_where_clause = new_where_clause.replace(old_pattern, new_pattern)

    # Handling IN operator
    number_regexp = """[0-9]+"""
    char_regexp = """[a-zA-Z1-9_]"""
    variable_regexp = """%s+""" % (char_regexp)
    string_regexp = """\"%s*\"""" % (char_regexp)
    string_or_number_regexp = """(%s|%s)""" % (number_regexp, string_regexp)
    regexp = """%s in \((%s)(,%s)*\)""" % (variable_regexp,
                                           string_or_number_regexp,
                                           string_or_number_regexp)
    in_expression_matches = re.search(regexp, new_where_clause)
    if in_expression_matches:
        non_none_in_expression_match = in_expression_matches.group()
        variable = non_none_in_expression_match.split("in")[0]
        list_value = non_none_in_expression_match.split("in")[1]
        values_match = re.findall(string_or_number_regexp, list_value)
        if values_match:
            non_none_matches = filter(lambda x: x != "", values_match)
            equivalent_using_or = map(lambda x: "%s == %s" % (variable, x), non_none_matches)
            equivalent_expr = "(%s)" % (" or ".join(equivalent_using_or))
            new_where_clause = new_where_clause.replace(non_none_in_expression_match, equivalent_expr)

    # Filter data according to where clause.
    result = result.fillna(value=0)
    filtered_result = result.query(new_where_clause) if new_where_clause != "1==1" else result

    # Filter duplicate tuples (ie "select A.x from A join B")
    selected_attributes_corrected = map(
        lambda a: a.replace("\"", "").replace(".", "__"), query_tree.attributes)
    table_in_selected_attributes = list(set(map(
        lambda x: x.split(".")[0].replace("\"", ""), query_tree.attributes)))
    id_columns = map(lambda x: "%s__id" % (x), table_in_selected_attributes)

    # Transform pandas data into dict.
    possible_final_columns = list(set(
        map(lambda l: "%s__id" % (l), labels)).intersection(filtered_result))
    final_columns = list(set(possible_final_columns).intersection(id_columns))

    final_tables = map(lambda x: x.split("__")[0], final_columns)
    filtered_result = filtered_result[final_columns].drop_duplicates()
    rows = []
    for each in filtered_result.itertuples():
        try:
            row = {}
            for (table_name, object_id) in zip(reversed(final_tables), reversed(each)):
                row[table_name] = table_id_index[table_name][object_id]
        except Exception:
            traceback.print_exc()
            pass
        rows += [row]

    # Process function calls
    if len(query_tree.function_calls) > 0:
        row = rows[0]
        for attribute_index, function_name in query_tree.function_calls.iteritems(
        ):
            attribute_name = selected_attributes_corrected[attribute_index]
            original_attribute_name = query_tree.attributes[attribute_index]

            if attribute_name in filtered_result:
                dataset = filtered_result[attribute_name]
            else:
                entity_target = original_attribute_name.split(".")[0]
                attribute_target = original_attribute_name.split(".")[1]

                data = map(lambda x: x[entity_target][attribute_target], rows)
                dataset = pd.DataFrame(data=data, columns=["column"])["column"]

            function = getattr(dataset, function_name)
            value = function()

            row_key = "%s(%s)" % (function_name, original_attribute_name)
            row[row_key] = value
        return [row]

    # Reduce results
    return rows
