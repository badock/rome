from __future__ import print_function
import sqlparse
import uuid
import logging
from sqlparse.sql import Token, IdentifierList, Identifier, Where, Comparison, Parenthesis, Function
import re

SELECT_PART = 1
FROM_PART = 2
WHERE_PART = 3


class QueryParserResult(object):

    def __init__(self):
        self.attributes = []
        self.models = []
        self.where_clauses = []
        self.joining_clauses = []
        self.variables = {}
        self.aliases = {}
        self.function_calls = {}


def correct_invalid_property(term):
    word_pattern = "[_a-zA-Z0-9]+"
    for x in ["\"%s\".\"%s\"", "%s.\"%s\""]:
        property_pattern = x % (word_pattern, word_pattern)
        match = re.search(property_pattern, term)
        if match is not None:
            table_name = match.group().split(".")[0].replace("\"", "")
            property_name = match.group().split(".")[1].replace("\"", "")
            term = term.replace(match.group(), "%s.%s" % (table_name, property_name))
    return term


class QueryParser(object):

    def __init__(self):
        pass

    def parse_select_identifier_list(self, identifier_candidates, query):
        for identifier_candidate in identifier_candidates.tokens:
            self.parse_select_identifier(identifier_candidate, query)
        return query

    def parse_select_function_identifier(self, function, query):
        # Here we assume that each function call follows this pattern:
        #   token[0] is the identifier of the function (count, avg, sum, ...)
        #   token[1] is a parenthesis containing the identifier of the field to which the function is applied
        function_name = function.tokens[0].value
        attribute = function.tokens[1].tokens[1]
        attribute_index = len(query.attributes)
        self.parse_select_identifier(attribute, query)
        query.function_calls[attribute_index] = function_name
        return query

    def parse_select_identifier(self, id_candidate, query):
        if type(id_candidate) is Identifier and id_candidate.value.strip() != "":
            if len(id_candidate.tokens) > 0 and type(id_candidate.tokens[0]) is Function:
                self.parse_select_function_identifier(
                    id_candidate.tokens[0], query)
            else:
                query.attributes += [id_candidate.value]
        return query

    def parse_from_identifier_list(self, identifier_candidates, query):
        for identifier_candidate in identifier_candidates.tokens:
            self.parse_from_identifier(identifier_candidate, query)
        return query

    def parse_from_identifier(self, id_candidate, query):
        if type(id_candidate) is Identifier and id_candidate.value.strip() != "":
            GETTING_TABLE_NAME = 0
            FIND_AS_TOKEN = 1
            GETTING_ALIAS_NAME = 2

            current_step = GETTING_TABLE_NAME
            tablename = None
            alias_name = None
            for token in id_candidate.tokens:
                if current_step == GETTING_TABLE_NAME:
                    if token.value != "":
                        tablename = token.value.replace("\"", "")
                        current_step = FIND_AS_TOKEN
                elif current_step is FIND_AS_TOKEN:
                    if token.value == "AS":
                        current_step = GETTING_ALIAS_NAME
                    else:
                        continue
                elif current_step == GETTING_ALIAS_NAME:
                    if token.value != "":
                        alias_name = token.value.replace("\"", "")
            query.models += [tablename]
            if alias_name:
                query.aliases[alias_name] = tablename
        return query

    def parse_where_clause(self, where_terms, query, joining_clause=False):

        def parse_parenthesis(parenthesis_term, query):
            parenthesis_terms = parenthesis_term.tokens[1:-1]
            first_term = parenthesis_terms[0]
            if type(first_term) is Token and first_term.value.upper() == "SELECT":
                nested_query = self.parse_select(parenthesis_terms)
                query_id = uuid.uuid4()
                query_name = "__%s__" % (query_id)
                token_id = Identifier(query_name)
                query.variables[query_name] = nested_query
                return token_id
            return parenthesis_term

        if type(
            where_terms.tokens[0]) is Token and where_terms.tokens[0].value == "WHERE":
            where_expression_terms = where_terms.tokens[1:]
        else:
            where_expression_terms = where_terms.tokens[0:]
        new_terms = []
        for term in where_expression_terms:
            if type(term) is Parenthesis:
                new_term = "%s " % (parse_parenthesis(term, query))
            elif type(term) is Comparison:
                new_term = "%s " % (term)
            elif type(term) is Token:
                new_term = "%s " % (term.value.strip())
            elif type(term) is Identifier:
                new_term = "%s " % (term.value.strip())
            else:
                logging.warning("Could not understand the following term: '%s'"
                                % (term))
                new_term = None
            if new_term is not None:
                new_terms += [new_term]
        new_terms_as_string = "".join(new_terms).strip()
        new_terms_as_string = correct_invalid_property(new_terms_as_string)
        if not joining_clause:
            query.where_clauses += [new_terms_as_string]
        else:
            query.joining_clauses += [new_terms_as_string]
        return query

    def parse_select(self, terms):
        query = QueryParserResult()
        parts_identifier = {
            "SELECT": SELECT_PART,
            "FROM": FROM_PART,
            "WHERE": WHERE_PART,
        }
        expected_part = -1
        for term in terms:

            # Try to detect if the term is part of the SELECT, FROM or WHERE
            if type(term) is Token:
                if term.value.upper() in ["SELECT", "FROM", "WHERE]"]:
                    expected_part = parts_identifier[term.value.upper()]
            elif type(term) is Where:
                expected_part = WHERE_PART
            else:
                pass

            # Analyse the term with the corresponding sub function
            if type(term) is Token:
                if term.value == "*" and expected_part == SELECT_PART:
                    query.attributes = ["*"]
                else:
                    pass
            elif type(term) is IdentifierList and expected_part == SELECT_PART:
                self.parse_select_identifier_list(term, query)
            elif type(term) is Identifier and expected_part == SELECT_PART:
                self.parse_select_identifier(term, query)
            elif type(term) is IdentifierList and expected_part == FROM_PART:
                self.parse_from_identifier_list(term, query)
            elif type(term) is Identifier and expected_part == FROM_PART:
                self.parse_from_identifier(term, query)
            elif type(term) is Where and expected_part == WHERE_PART:
                self.parse_where_clause(term, query)
            elif type(term) is Comparison and expected_part == FROM_PART:
                self.parse_where_clause(term, query, joining_clause=True)
            else:
                logging.warning(
                    "The following term could not be recognized: '%s'" % (term))
                pass
        return query

    def parse(self, query):
        parsed = sqlparse.parse(query)[0]
        if parsed.get_type() == "UNKNOWN":
            raise Exception("Query '%s' is invalid" % (query))
        elif parsed.get_type() == "SELECT":
            query = self.parse_select(parsed.tokens)
            return query
        return None


if __name__ == "__main__":
    parser = QueryParser()
    print(parser.parse("select * from foo"))
    query = parser.parse("""SELECT Id, FirstName, LastName, Country
  FROM Customer
 WHERE Country IN
       (SELECT Country
          FROM Supplier)""")
    print(query)
    query = parser.parse("select * from foo where x in (1, 2, 3)")
    print(query)
    query = parser.parse(
        "select * from foo where x in (select * from foo where x in (select * from foo where x in (1, 2, 3))) and y = 1")
    print(query)
