import sqlparse
import uuid
import logging

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


class QueryParser(object):

    def __init__(self):
        pass

    def parse_select_identifier_list(self, identifier_candidates, query):
        for identifier_candidate in identifier_candidates.tokens:
            self.parse_select_identifier(identifier_candidate, query)
        return query

    def parse_select_identifier(self, identifier_candidate, query):
        if type(identifier_candidate) is sqlparse.sql.Identifier and identifier_candidate.value.strip() != "":
            query.attributes += [identifier_candidate.value]
        return query

    def parse_from_identifier_list(self, identifier_candidates, query):
        for identifier_candidate in identifier_candidates.tokens:
            self.parse_from_identifier(identifier_candidate, query)
        return query

    def parse_from_identifier(self, identifier_candidate, query):
        if type(identifier_candidate) is sqlparse.sql.Identifier and identifier_candidate.value.strip() != "":
            GETTING_TABLE_NAME= 0
            FIND_AS_TOKEN = 1
            GETTING_ALIAS_NAME = 2

            current_step = GETTING_TABLE_NAME
            tablename = None
            alias_name = None
            for token in identifier_candidate.tokens:
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
            first_parenthesis_term = parenthesis_terms[0]
            if type(first_parenthesis_term) is sqlparse.sql.Token and first_parenthesis_term.value.upper() == "SELECT":
                nested_query = self.parse_select(parenthesis_terms)
                query_id = uuid.uuid4()
                query_name = "__%s__" % (query_id)
                token_id = sqlparse.sql.Identifier(query_name)
                query.variables[query_name] = nested_query
                return token_id
            return parenthesis_term
        if type(where_terms.tokens[0]) is sqlparse.sql.Token and where_terms.tokens[0].value == "WHERE":
            where_expression_terms = where_terms.tokens[1:]
        else:
            where_expression_terms = where_terms.tokens[0:]
        new_terms = []
        for term in where_expression_terms:
            if type(term) is sqlparse.sql.Parenthesis:
                new_term = "%s " % (parse_parenthesis(term, query))
            elif type(term) is sqlparse.sql.Comparison:
                new_term = "%s " % (term)
            elif type(term) is sqlparse.sql.Token:
                new_term = "%s " % (term.value.strip())
            elif type(term) is sqlparse.sql.Identifier:
                new_term = "%s " % (term.value.strip())
            else:
                logging.warning("Could not understand the following term: '%s'" % (term))
                new_term = None
            if new_term is not None:
                new_terms += [new_term]
        new_terms_as_string = "".join(new_terms).strip()
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
        expecting_part = -1
        for term in terms:

            # Try to detect if the term is part of the SELECT, FROM or WHERE
            if type(term) is sqlparse.sql.Token:
                if term.value.upper() in ["SELECT", "FROM", "WHERE]"]:
                    expecting_part = parts_identifier[term.value.upper()]
            elif type(term) is sqlparse.sql.Where:
                expecting_part = WHERE_PART
            else:
                pass

            # Analyse the term with the corresponding sub function
            if type(term) is sqlparse.sql.Token:
                if term.value == "*" and expecting_part == SELECT_PART:
                    query.attributes = ["*"]
                else:
                    pass
            elif type(term) is sqlparse.sql.IdentifierList and expecting_part == SELECT_PART:
                self.parse_select_identifier_list(term, query)
            elif type(term) is sqlparse.sql.Identifier and expecting_part == SELECT_PART:
                self.parse_select_identifier(term, query)
            elif type(term) is sqlparse.sql.IdentifierList and expecting_part == FROM_PART:
                self.parse_from_identifier_list(term, query)
            elif type(term) is sqlparse.sql.Identifier and expecting_part == FROM_PART:
                self.parse_from_identifier(term, query)
            elif type(term) is sqlparse.sql.Where and expecting_part == WHERE_PART:
                self.parse_where_clause(term, query)
            elif type(term) is sqlparse.sql.Comparison and expecting_part == FROM_PART:
                self.parse_where_clause(term, query, joining_clause=True)
            else:
                logging.warning("The following term could not be recognized: '%s'" % (term))
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
    query = parser.parse("select * from foo where x in (select * from foo where x in (select * from foo where x in (1, 2, 3))) and y = 1")
    print(query)
