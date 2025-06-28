# ruff: noqa: W291

DEEP_FILTER_AND_ROOT = {
    "params": (
        "select(id);and(like(foreign2.foo.name,'a%'),eq(another_id,1))",
        (
            "SELECT my_model.id \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id2 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id \n"
            "WHERE like(foo_1.name, :like_1) AND my_model.another_id = :another_id_1"
        ),
    ),
    "id": "deep_filter_and_root",
}

NESTED_PROJECTION = {
    "params": (
        "select(id,foreign2[foo[name]])",
        (
            "SELECT foo_1.id, foo_1.name, foreign_1.id AS id_1, my_model.id AS id_2 \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id2 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id"
        ),
    ),
    "id": "nested_projection",
}

DEEP_LIKE = {
    "params": (
        "select(id);like(foreign2.foo.name,'b%')",
        (
            "SELECT my_model.id \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id2 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id \n"
            "WHERE like(foo_1.name, :like_1)"
        ),
    ),
    "id": "deep_like",
}

OR_EQS = {
    "params": (
        "select(id);or(eq(foreign1.name,'aaa'),eq(foreign1.name,'bbb'))",
        (
            "SELECT my_model.id \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 \n'
            "WHERE foreign_1.name = :name_1 OR foreign_1.name = :name_2"
        ),
    ),
    "id": "or_eqs",
}

NOT_EQS = {
    "params": (
        "select(id);not(eq(foreign1.name,'aaa'))",
        (
            "SELECT my_model.id \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 \n'
            "WHERE foreign_1.name != :name_1"
        ),
    ),
    "id": "not_eqs",
}

BOOLEAN_EQ = {
    "params": (
        "select(id);eq(is_boolean,true)",
        """
SELECT my_model.id 
FROM my_model 
WHERE my_model.is_boolean = true
""",
    ),
    "id": "boolean_eq",
}

SELECT_NESTED_FIELD = {
    "params": (
        "select(id,foreign1[name])",
        (
            "SELECT foreign_1.name, foreign_1.id, my_model.id AS id_1 \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1'
        ),
    ),
    "id": "select_nested_field",
}

IN_LIKE_JOIN = {
    "params": (
        "select(id);and(in(id,(1,2,3)),like(foreign1.name,'asda'))",
        (
            "SELECT my_model.id \n"
            'FROM my_model LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 \n'
            "WHERE my_model.id IN (__[POSTCOMPILE_id_1]) AND like(foreign_1.name, :like_1)"
        ),
    ),
    "id": "in-like-join",
}

DEEP_NESTED_WITH_SORT_AND_OR = {
    "params": (
        "select(id,foreign1[id,foo],foreign2[*,foo[id]]);sort(foreign1.id);or(in(id,(1,2,3)),like(foreign1.name,'name'))",
        (
            "SELECT"
            " foo_1.id,"
            " foo_1.name,"
            " foreign_1.id AS id_1,"
            " foo_2.id AS id_2,"
            " foreign_2.name AS name_1,"
            " foreign_2.foo_id,"
            " foreign_2.id AS id_3,"
            " my_model.id AS id_4 \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id "
            'LEFT OUTER JOIN "foreign" AS foreign_2 ON foreign_2.id = my_model.foreign_id2 '
            "LEFT OUTER JOIN foo AS foo_2 ON foo_2.id = foreign_2.foo_id \n"
            "WHERE my_model.id IN (__[POSTCOMPILE_id_5]) OR like(foreign_1.name, :like_1) "
            "ORDER BY foreign_1.id ASC"
        ),
    ),
    "id": "deep_nested_with_sort_and_or",
}

# ruff: noqa: W291


DISTINCT_SIMPLE = {
    "params": ("select(id);distinct(id)", "SELECT my_model.id \nFROM my_model"),
    "id": "distinct_simple",
}

SORT_DESC = {
    "params": ("select(id);sort(-id)", "SELECT my_model.id \nFROM my_model ORDER BY -my_model.id"),
    "id": "sort_desc",
}

SORT_ASC = {
    "params": ("select(id);sort(+id)", "SELECT my_model.id \nFROM my_model ORDER BY my_model.id ASC"),
    "id": "sort_asc",
}

DEEP_FILTER = {
    "params": (
        "select(id);eq(foreign1.foo.id,2)",
        (
            "SELECT my_model.id \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id \n"
            "WHERE foo_1.id = :id_1"
        ),
    ),
    "id": "deep_filter",
}

DEEP_FILTER_WITH_OR = {
    "params": (
        "select(id);or(eq(int_number,2),eq(nullable,5))",
        (
            "SELECT my_model.id \n"
            "FROM my_model \n"
            "WHERE my_model.int_number = :int_number_1 OR my_model.nullable = :nullable_1"
        ),
    ),
    "id": "deep_filter_with_or",
}

DEEP_FILTER_WITH_AND = {
    "params": (
        "select(id);and(eq(int_number,2),eq(nullable,5))",
        (
            "SELECT my_model.id \n"
            "FROM my_model \n"
            "WHERE my_model.int_number = :int_number_1 AND my_model.nullable = :nullable_1"
        ),
    ),
    "id": "deep_filter_with_and",
}

LIKE_DEEP_FIELD = {
    "params": (
        "select(id);like(foreign1.foo.name,'abc%')",
        "SELECT my_model.id \n"
        "FROM my_model "
        'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 '
        "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id \n"
        "WHERE like(foo_1.name, :like_1)",
    ),
    "id": "like_deep_field",
}

SELECT_MANY_TO_MANY = {
    "params": (
        "select(id,many_to_many[*])",
        (
            "SELECT manytomanymodel_1.id, manytomanymodel_1.name, my_model.id AS id_1 \n"
            "FROM my_model "
            "LEFT OUTER JOIN "
            "(manytomanylinkmodel AS manytomanylinkmodel_1 "
            "JOIN manytomanymodel AS manytomanymodel_1 ON manytomanymodel_1.id = manytomanylinkmodel_1.many_id) "
            "ON my_model.id = manytomanylinkmodel_1.model_id"
        ),
    ),
    "id": "select_many_to_many",
}

SELECT_ALL_FIELDS = {
    "params": (
        "select(*)",
        (
            "SELECT"
            " my_model.created_at,"
            " my_model.another_id,"
            " my_model.int_number,"
            " my_model.float_number,"
            " my_model.is_boolean,"
            " my_model.nullable,"
            " my_model.foreign_id1,"
            " my_model.foreign_id2,"
            " my_model.id,"
            " my_model.parent_id \n"
            "FROM my_model"
        ),
    ),
    "id": "select_all_fields",
}

NESTED_MULTILEVEL_PROJECTION = {
    "params": (
        "select(id,foreign1[foo[name]])",
        (
            "SELECT foo_1.id, foo_1.name, foreign_1.id AS id_1, my_model.id AS id_2 \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id"
        ),
    ),
    "id": "nested_multilevel_projection",
}

AND_OR_COMBINATION = {
    "params": (
        "select(id);and(or(eq(nullable,1),eq(nullable,2)),eq(is_boolean,true))",
        (
            "SELECT my_model.id \n"
            "FROM my_model \n"
            "WHERE (my_model.nullable = :nullable_1 OR my_model.nullable = :nullable_2) AND my_model.is_boolean = true"
        ),
    ),
    "id": "and_or_combination",
}

FILTER_ON_CHILD = {
    "params": (
        "select(id);eq(children.int_number,5)",
        (
            "SELECT my_model.id \n"
            "FROM my_model "
            "LEFT OUTER JOIN my_model AS my_model_1 ON my_model.id = my_model_1.parent_id \n"
            "WHERE my_model_1.int_number = :int_number_1"
        ),
    ),
    "id": "filter_on_child",
}

SELECT_CHILD_PROJECTION = {
    "params": (
        "select(id,children[id])",
        (
            "SELECT my_model_1.id, my_model.id AS id_1 \n"
            "FROM my_model "
            "LEFT OUTER JOIN my_model AS my_model_1 ON my_model.id = my_model_1.parent_id"
        ),
    ),
    "id": "select_child_projection",
}

SELECT_CHILD_WILDCARD = {
    "params": (
        "select(id,children[*])",
        (
            "SELECT"
            " my_model_1.created_at,"
            " my_model_1.another_id,"
            " my_model_1.int_number,"
            " my_model_1.float_number,"
            " my_model_1.is_boolean,"
            " my_model_1.nullable,"
            " my_model_1.foreign_id1,"
            " my_model_1.foreign_id2,"
            " my_model_1.id, my_model_1.parent_id,"
            " my_model.id AS id_1 \n"
            "FROM my_model "
            "LEFT OUTER JOIN my_model AS my_model_1 ON my_model.id = my_model_1.parent_id"
        ),
    ),
    "id": "select_child_wildcard",
}

FILTER_NULL = {
    "params": (
        "select(id);eq(nullable,null)",
        ("SELECT my_model.id \nFROM my_model \nWHERE my_model.nullable IS NULL"),
    ),
    "id": "filter_null",
}

MULTI_FIELD_SORT = {
    "params": (
        "select(id);sort(+int_number,-float_number)",
        (
            "SELECT my_model.int_number, my_model.float_number, my_model.id \n"
            "FROM my_model "
            "ORDER BY my_model.int_number ASC, -my_model.float_number"
        ),
    ),
    "id": "multi_field_sort",
}
MULTI_FIELD_NESTED_SORT = {
    "params": (
        "select(id);sort(+foreign1.id,-foreign1.name)",
        (
            "SELECT foreign_1.name, foreign_1.id, my_model.id AS id_1 \n"
            'FROM my_model LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 '
            "ORDER BY foreign_1.id ASC, -foreign_1.name"
        ),
    ),
    "id": "multi_field_nested_sort",
}

SEARCH_STRING_FIELD = {
    "params": (
        "select(id);like(foreign1.name,'%abc%')",
        (
            "SELECT my_model.id \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 \n'
            "WHERE like(foreign_1.name, :like_1)"
        ),
    ),
    "id": "search_string_field",
}

NESTED_FILTER_AND_SORT = {
    "params": (
        "select(id,foreign2[foo[id]]);eq(foreign2.foo.name,'x');sort(+foreign2.foo.id)",
        (
            "SELECT foo_1.id, foreign_1.id AS id_1, my_model.id AS id_2 \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id2 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id \n"
            "WHERE foo_1.name = :name_1 "
            "ORDER BY foo_1.id ASC"
        ),
    ),
    "id": "nested_filter_and_sort",
}
NESTED_WILDCARD_AND_FIELD_PROJECTION = {
    "params": (
        "select(id,foreign2[*,foo[name]])",
        (
            "SELECT foo_1.id, foo_1.name, foreign_1.name AS name_1, foreign_1.foo_id, "
            "foreign_1.id AS id_1, my_model.id AS id_2 \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id2 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id"
        ),
    ),
    "id": "nested_wildcard_and_field_projection",
}

FILTER_DEEP_FIELD_SORT_ROOT = {
    "params": (
        "select(id);eq(foreign1.foo.name,'abc');sort(+id)",
        (
            "SELECT my_model.id \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id \n"
            "WHERE foo_1.name = :name_1 ORDER BY my_model.id ASC"
        ),
    ),
    "id": "filter_deep_field_sort_root",
}

SELECT_DEEP_FIELD_ONLY = {
    "params": (
        "select(foreign2[foo[name]]);eq(foreign2.foo.name,'abc')",
        (
            "SELECT foo_1.id, foo_1.name, foreign_1.id AS id_1, my_model.id AS id_2 \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id2 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id \n"
            "WHERE foo_1.name = :name_1"
        ),
    ),
    "id": "select_deep_field_only",
}
NES1 = {
    "params": (
        "select(foreign2[foo[name]]);",
        (
            "SELECT foo_1.id, foo_1.name, foreign_1.id AS id_1, my_model.id AS id_2 \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id2 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id \n"
        ),
    ),
    "id": "Nes1",
}

NESTED_RELATION_NO_WILDCARD = {
    "params": (
        "select(foreign1)",
        (
            "SELECT foreign_1.name, foreign_1.foo_id, foreign_1.id, my_model.id AS id_1 \n"
            'FROM my_model LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1'
        ),
    ),
    "id": "nested_relation_no_wildcard",
}
NESTED_RELATION_NO_WILDCARD2 = {
    "params": (
        "select(foreign1[name,foo])",
        (
            "SELECT foo_1.id, foo_1.name, foreign_1.name AS name_1, foreign_1.id AS id_1, my_model.id AS id_2 \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id"
        ),
    ),
    "id": "nested_relation_no_wildcard2",
}

NESTED_RELATION_NESTED_WILDCARD = {
    "params": (
        "select(foreign1[*,foo])",
        (
            "SELECT"
            " foo_1.id,"
            " foo_1.name,"
            " foreign_1.name AS name_1,"
            " foreign_1.foo_id,"
            " foreign_1.id AS id_1,"
            " my_model.id AS id_2 \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_1.foo_id"
        ),
    ),
    "id": "nested_relation_nested_wildcard",
}

NESTED_RELATION_NO_WILDCARD4 = {
    "params": (
        "select(*,foreign1)",
        (
            "SELECT"
            " foreign_1.name,"
            " foreign_1.foo_id,"
            " foreign_1.id,"
            " my_model.created_at,"
            " my_model.another_id,"
            " my_model.int_number,"
            " my_model.float_number,"
            " my_model.is_boolean,"
            " my_model.nullable,"
            " my_model.foreign_id1,"
            " my_model.foreign_id2,"
            " my_model.id AS id_1,"
            " my_model.parent_id \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1'
        ),
    ),
    "id": "nested_relation_no_wildcard4",
}

WILDCARD_AND_EXPLICIT_NESTED_SORT = {
    "params": (
        "select(id,foreign1[*],foreign2[foo[id]]);sort(+foreign2.foo.id)",
        (
            "SELECT"
            " foreign_1.name,"
            " foreign_1.foo_id,"
            " foreign_1.id,"
            " foo_1.id AS id_1,"
            " foreign_2.id AS id_2,"
            " my_model.id AS id_3 \n"
            "FROM my_model "
            'LEFT OUTER JOIN "foreign" AS foreign_1 ON foreign_1.id = my_model.foreign_id1 '
            'LEFT OUTER JOIN "foreign" AS foreign_2 ON foreign_2.id = my_model.foreign_id2 '
            "LEFT OUTER JOIN foo AS foo_1 ON foo_1.id = foreign_2.foo_id "
            "ORDER BY foo_1.id ASC"
        ),
    ),
    "id": "wildcard_and_explicit_nested_sort",
}

FILTER_DATE = {
    "params": (
        "select(id);eq(created_at,2025-06-10)",
        ("SELECT my_model.id \nFROM my_model \nWHERE my_model.created_at = :created_at_1"),
    ),
    "id": "filter_date",
}
FILTER_TIME = {
    "params": (
        "select(id);eq(created_at,08:30:00)",
        ("SELECT my_model.id \nFROM my_model \nWHERE my_model.created_at = :created_at_1"),
    ),
    "id": "filter_time",
}
FILTER_DATETIME = {
    "params": (
        "select(id);eq(created_at,2025-06-10T08:30:00)",
        ("SELECT my_model.id \nFROM my_model \nWHERE my_model.created_at = :created_at_1"),
    ),
    "id": "filter_datetime",
}

# ruff: noqa: W291

ALL_TEST_CASES = [
    NES1,
    NESTED_RELATION_NO_WILDCARD,
    NESTED_RELATION_NO_WILDCARD2,
    NESTED_RELATION_NESTED_WILDCARD,
    NESTED_RELATION_NO_WILDCARD4,
    FILTER_DATE,
    FILTER_TIME,
    FILTER_DATETIME,
    NESTED_WILDCARD_AND_FIELD_PROJECTION,
    FILTER_DEEP_FIELD_SORT_ROOT,
    SELECT_DEEP_FIELD_ONLY,
    # UNWIND_NESTED_AND_FILTER,
    WILDCARD_AND_EXPLICIT_NESTED_SORT,
    DEEP_FILTER_AND_ROOT,
    NESTED_PROJECTION,
    DEEP_LIKE,
    DEEP_FILTER,
    OR_EQS,
    NOT_EQS,
    BOOLEAN_EQ,
    SELECT_NESTED_FIELD,
    IN_LIKE_JOIN,
    DEEP_NESTED_WITH_SORT_AND_OR,
    # DISTINCT_SIMPLE,
    SORT_DESC,
    SORT_ASC,
    DEEP_FILTER_WITH_OR,
    DEEP_FILTER_WITH_AND,
    LIKE_DEEP_FIELD,
    SELECT_MANY_TO_MANY,
    SELECT_ALL_FIELDS,
    NESTED_MULTILEVEL_PROJECTION,
    AND_OR_COMBINATION,
    FILTER_ON_CHILD,
    SELECT_CHILD_PROJECTION,
    SELECT_CHILD_WILDCARD,
    FILTER_NULL,
    MULTI_FIELD_SORT,
    MULTI_FIELD_NESTED_SORT,
    SEARCH_STRING_FIELD,
    NESTED_FILTER_AND_SORT,
]
