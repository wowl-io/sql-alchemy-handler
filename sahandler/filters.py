from abc import ABC, abstractmethod
from sqlalchemy import *
from sqlalchemy.orm import aliased
from urllib.parse import unquote


class BaseQueryFilter(ABC):
    is_join_filter = False

    def __init__(self, model, filter_key, filter_value):
        self._model = model
        self._filter_key = filter_key
        self._filter_value = filter_value
        self._column = None
        self._operator = None

    def is_valid_column(self, model):
        return hasattr(model, self._column)

    def get_column(self):
        return self._column

    def get_operator(self):
        return self._operator

    @staticmethod
    def get_list(value):
        if not value:
            return []
        values = [value]
        if "," in value:
            values = value.split(",")
        return [unquote(v) for v in values]

    @abstractmethod
    def add_to_query(self, query):
        pass


class DefaultFilter(BaseQueryFilter):
    def add_to_query(self, query):
        if "__" in self._filter_key:
            self._column, self._operator = self._filter_key.split("__")
            if self.is_valid_column(self._model):
                if self._operator == "in":
                    return query.filter(getattr(self._model, self._column).in_(self.get_list(self._filter_value)))
                if self._operator == "exclude":
                    return query.filter(getattr(self._model, self._column).notin_(self.get_list(self._filter_value)))
                if self._operator == "contains":
                    return query.filter(getattr(self._model, self._column).like("%%%s%%" % str(self._filter_value)))
                if self._operator == "startswith":
                    return query.filter(getattr(self._model, self._column).like("%s%%" % str(self._filter_value)))
                if self._operator == "endswith":
                    return query.filter(getattr(self._model, self._column).like("%%%s" % str(self._filter_value)))
                if self._operator == "soundex":
                    return query.filter(getattr(self._model, self._column).op("SOUNDS LIKE")(str(self._filter_value)))
                if self._operator == "gte":
                    return query.filter(getattr(self._model, self._column) >= self._filter_value)
                if self._operator == "gt":
                    return query.filter(getattr(self._model, self._column) > self._filter_value)
                if self._operator == "lte":
                    return query.filter(getattr(self._model, self._column) <= self._filter_value)
                if self._operator == "lt":
                    return query.filter(getattr(self._model, self._column) < self._filter_value)
        self._column = self._filter_key
        self._operator = "eq"
        return query.filter(getattr(self._model, self._filter_key) == self._filter_value)


class OrFilter(BaseQueryFilter):
    def add_to_query(self, query):
        expressions = []
        if "__" in self._filter_key:
            self._column, self._operator = self._filter_key.split("__")
            columns = self._column.split("_or_")
            for c in columns:
                if self._operator == "in":
                    expressions.append(getattr(self._model, c).in_(self.get_list(self._filter_value)))
                if self._operator == "exclude":
                    expressions.append(getattr(self._model, c).notin_(self.get_list(self._filter_value)))
                if self._operator == "contains":
                    expressions.append(getattr(self._model, c).like("%%%s%%" % str(self._filter_value)))
                if self._operator == "startswith":
                    expressions.append(getattr(self._model, c).like("%s%%" % str(self._filter_value)))
                if self._operator == "endswith":
                    expressions.append(getattr(self._model, c).like("%%%s" % str(self._filter_value)))
                if self._operator == "soundex":
                    expressions.append(getattr(self._model, c).op("SOUNDS LIKE")(str(self._filter_value)))
                if self._operator == "gte":
                    expressions.append(getattr(self._model, c) >= self._filter_value)
                if self._operator == "gt":
                    expressions.append(getattr(self._model, c) > self._filter_value)
                if self._operator == "lte":
                    expressions.append(getattr(self._model, c) <= self._filter_value)
                if self._operator == "lt":
                    expressions.append(getattr(self._model, c) < self._filter_value)
            return query.filter(or_(*expressions))
        self._column = self._filter_key
        self._operator = "eq"
        columns = self._column.split("_or_")
        for c in columns:
            expressions.append(getattr(self._model, c) == self._filter_value)
        return query.filter(or_(*expressions))


class BaseJoinFilter(BaseQueryFilter):
    is_join_filter = True

    def __init__(self, model, filter_key, filter_value):
        super().__init__(model, filter_key, filter_value)
        self._intermediate_model = None
        self._secondary_model = None
        self._intermediate_model_alias = None
        self._secondary_model_alias = None
        self._model_to_intermediate_relation = None
        self._intermediate_to_secondary_relation = None
        self._model_to_secondary_relation = None
        self._default_column = None

    def set_intermediate_model(self, model):
        self._intermediate_model = model
        return self

    def set_secondary_model(self, model):
        self._secondary_model = model
        return self

    def set_model_to_intermediate_relation(self, relation):
        self._model_to_intermediate_relation = str(relation)
        return self

    def set_intermediate_to_secondary_relation(self, relation):
        self._intermediate_to_secondary_relation = str(relation)
        return self

    def set_model_to_secondary_relation(self, relation):
        self._model_to_secondary_relation = str(relation)
        return self

    def get_intermediate_model_alias(self):
        if not self._intermediate_model_alias:
            self._intermediate_model_alias = aliased(
                self._intermediate_model,
                name="%s_%s" % (self._intermediate_model.__tablename__, self._filter_key)
            )
        return self._intermediate_model_alias

    def get_secondary_model_alias(self):
        if not self._secondary_model_alias:
            self._secondary_model_alias = aliased(
                self._secondary_model,
                name="%s_%s" % (self._secondary_model.__tablename__, self._filter_key)
            )
        return self._secondary_model_alias

    def set_default_column(self, column):
        self._default_column = column
        return self


class OneToOneJoinFilter(BaseJoinFilter):
    def add_to_query(self, query):
        key_fields = self._filter_key.split("__")
        self._column = key_fields[1]
        query = query.join(
            self.get_secondary_model_alias(),
            getattr(self._model, self._model_to_secondary_relation)
        )
        if len(key_fields) == 3:
            self._operator = key_fields[2]
            if self.is_valid_column(self._secondary_model):
                if self._operator == "in":
                    return query.filter(
                        getattr(
                            self.get_secondary_model_alias(), self._column
                        ).in_(self.get_list(self._filter_value))
                    )
                if self._operator == "exclude":
                    return query.filter(
                        getattr(
                            self.get_secondary_model_alias(), self._column
                        ).notin_(self.get_list(self._filter_value))
                    )
                if self._operator == "contains":
                    return query.filter(
                        getattr(self.get_secondary_model_alias(), self._column).like("%%%s%%" % str(self._filter_value))
                    )
                if self._operator == "startswith":
                    return query.filter(
                        getattr(self.get_secondary_model_alias(), self._column).like("%s%%" % str(self._filter_value))
                    )
                if self._operator == "endswith":
                    return query.filter(
                        getattr(self.get_secondary_model_alias(), self._column).like("%%%s" % str(self._filter_value))
                    )
                if self._operator == "soundex":
                    return query.filter(
                        getattr(self.get_secondary_model_alias(), self._column).op(
                            "SOUNDS LIKE"
                        )(str(self._filter_value))
                    )
                if self._operator == "gte":
                    return query.filter(getattr(self.get_secondary_model_alias(), self._column) >= self._filter_value)
                if self._operator == "gt":
                    return query.filter(getattr(self.get_secondary_model_alias(), self._column) > self._filter_value)
                if self._operator == "lte":
                    return query.filter(getattr(self.get_secondary_model_alias(), self._column) <= self._filter_value)
                if self._operator == "lt":
                    return query.filter(getattr(self.get_secondary_model_alias(), self._column) < self._filter_value)
        self._operator = "eq"
        return query.filter(getattr(self.get_secondary_model_alias(), self._column) == self._filter_value)


class OneToManyJoinFilter(BaseJoinFilter):
    def add_to_query(self, query):
        query = query.join(
            self.get_secondary_model_alias(),
            getattr(self._model, self._model_to_secondary_relation)
        )
        if "__" in self._filter_key:
            key_fields = self._filter_key.split("__")
            self._column = key_fields[1]
            if len(key_fields) == 3:
                self._operator = key_fields[2]
                if self.is_valid_column(self._secondary_model):
                    if self._operator == "in":
                        return query.filter(
                            getattr(
                                self.get_secondary_model_alias(), self._column
                            ).in_(
                                self.get_list(self._filter_value)
                            )
                        )
                    if self._operator == "exclude":
                        return query.filter(
                            getattr(
                                self.get_secondary_model_alias(), self._column
                            ).notin_(
                                self.get_list(self._filter_value)
                            )
                        )
                    if self._operator == "contains":
                        return query.filter(
                            getattr(self.get_secondary_model_alias(), self._column).like(
                                "%%%s%%" % str(self._filter_value)
                            )
                        )
                    if self._operator == "startswith":
                        return query.filter(
                            getattr(self.get_secondary_model_alias(), self._column).like(
                                "%s%%" % str(self._filter_value)
                            )
                        )
                    if self._operator == "endswith":
                        return query.filter(
                            getattr(self.get_secondary_model_alias(), self._column).like(
                                "%%%s" % str(self._filter_value)
                            )
                        )
                    if self._operator == "soundex":
                        return query.filter(
                            getattr(self.get_secondary_model_alias(), self._column).op(
                                "SOUNDS LIKE"
                            )(str(self._filter_value))
                        )
                    if self._operator == "gte":
                        return query.filter(
                            getattr(self.get_secondary_model_alias(), self._column) >= self._filter_value
                        )
                    if self._operator == "gt":
                        return query.filter(
                            getattr(self.get_secondary_model_alias(), self._column) > self._filter_value
                        )
                    if self._operator == "lte":
                        return query.filter(
                            getattr(self.get_secondary_model_alias(), self._column) <= self._filter_value
                        )
                    if self._operator == "lt":
                        return query.filter(
                            getattr(self.get_secondary_model_alias(), self._column) < self._filter_value
                        )
            self._operator = "eq"
            return query.filter(getattr(self.get_secondary_model_alias(), self._column) == self._filter_value)
        self._operator = "in"
        return query.filter(
            getattr(self.get_secondary_model_alias(), self._default_column).in_(self.get_list(self._filter_value))
        )


class OneToManyKeyValueJoinFilter(BaseJoinFilter):
    def __init__(self, model, filter_key, filter_value):
        super().__init__(model, filter_key, filter_value)
        self._key_field = None
        self._value_field = None

    def set_key_field(self, field):
        self._key_field = field
        return self

    def set_value_field(self, field):
        self._value_field = field
        return self

    def add_to_query(self, query):
        query = query.join(
            self.get_secondary_model_alias(),
            getattr(self._model, self._model_to_secondary_relation)
        )
        key_fields = self._filter_key.split("__")
        self._column = key_fields[1]
        if len(key_fields) == 3:
            self._operator = key_fields[2]
            if self._operator == "in":
                return query.filter(
                    getattr(
                        self.get_secondary_model_alias(), self._key_field
                    ) == self._column,
                    getattr(
                        self.get_secondary_model_alias(), self._value_field
                    ).in_(
                        self.get_list(self._filter_value)
                    )
                )
            if self._operator == "exclude":
                return query.filter(
                    getattr(
                        self.get_secondary_model_alias(), self._key_field
                    ) == self._column,
                    getattr(
                        self.get_secondary_model_alias(), self._value_field
                    ).notin_(
                        self.get_list(self._filter_value)
                    )
                )
            if self._operator == "contains":
                return query.filter(
                    getattr(
                        self.get_secondary_model_alias(), self._key_field
                    ) == self._column,
                    getattr(self.get_secondary_model_alias(), self._value_field).like(
                        "%%%s%%" % str(self._filter_value)
                    )
                )
            if self._operator == "startswith":
                return query.filter(
                    getattr(
                        self.get_secondary_model_alias(), self._key_field
                    ) == self._column,
                    getattr(self.get_secondary_model_alias(), self._value_field).like(
                        "%s%%" % str(self._filter_value)
                    )
                )
            if self._operator == "endswith":
                return query.filter(
                    getattr(
                        self.get_secondary_model_alias(), self._key_field
                    ) == self._column,
                    getattr(self.get_secondary_model_alias(), self._value_field).like(
                        "%%%s" % str(self._filter_value)
                    )
                )
            if self._operator == "soundex":
                return query.filter(
                    getattr(
                        self.get_secondary_model_alias(), self._key_field
                    ) == self._column,
                    getattr(self.get_secondary_model_alias(), self._value_field).op(
                        "SOUNDS LIKE"
                    )(str(self._filter_value))
                )
            if self._operator == "gte":
                return query.filter(
                    getattr(
                        self.get_secondary_model_alias(), self._key_field
                    ) == self._column,
                    getattr(self.get_secondary_model_alias(), self._value_field) >= self._filter_value
                )
            if self._operator == "gt":
                return query.filter(
                    getattr(
                        self.get_secondary_model_alias(), self._key_field
                    ) == self._column,
                    getattr(self.get_secondary_model_alias(), self._value_field) > self._filter_value
                )
            if self._operator == "lte":
                return query.filter(
                    getattr(
                        self.get_secondary_model_alias(), self._key_field
                    ) == self._column,
                    getattr(self.get_secondary_model_alias(), self._value_field) <= self._filter_value
                )
            if self._operator == "lt":
                return query.filter(
                    getattr(
                        self.get_secondary_model_alias(), self._key_field
                    ) == self._column,
                    getattr(self.get_secondary_model_alias(), self._value_field) < self._filter_value
                )
        self._operator = "eq"
        return query.filter(
            getattr(
                self.get_secondary_model_alias(), self._key_field
            ) == self._column,
            getattr(self.get_secondary_model_alias(), self._value_field) == self._filter_value
        )


class ManyToManyJoinFilter(BaseJoinFilter):
    def add_to_query(self, query):
        query = query.join(
            self.get_intermediate_model_alias(),
            getattr(self._model, self._model_to_intermediate_relation)
        )
        query = query.join(
            self.get_secondary_model_alias(),
            getattr(self.get_intermediate_model_alias(), self._intermediate_to_secondary_relation)
        )

        key_fields = self._filter_key.split("__")
        self._column = key_fields[1]
        if len(key_fields) == 3:
            self._operator = key_fields[2]
            if self.is_valid_column(self._secondary_model):
                if self._operator == "in":
                    return query.filter(
                        getattr(
                            self.get_secondary_model_alias(), self._column
                        ).in_(
                            self.get_list(self._filter_value)
                        )
                    )
                if self._operator == "exclude":
                    return query.filter(
                        getattr(
                            self.get_secondary_model_alias(), self._column
                        ).notin_(
                            self.get_list(self._filter_value)
                        )
                    )
                if self._operator == "contains":
                    return query.filter(
                        getattr(self.get_secondary_model_alias(), self._column).like(
                            "%%%s%%" % str(self._filter_value)
                        )
                    )
                if self._operator == "startswith":
                    return query.filter(
                        getattr(self.get_secondary_model_alias(), self._column).like(
                            "%s%%" % str(self._filter_value)
                        )
                    )
                if self._operator == "endswith":
                    return query.filter(
                        getattr(self.get_secondary_model_alias(), self._column).like(
                            "%%%s" % str(self._filter_value)
                        )
                    )
                if self._operator == "soundex":
                    return query.filter(
                        getattr(self.get_secondary_model_alias(), self._column).op(
                            "SOUNDS LIKE"
                        )(str(self._filter_value))
                    )
                if self._operator == "gte":
                    return query.filter(
                        getattr(self.get_secondary_model_alias(), self._column) >= self._filter_value
                    )
                if self._operator == "gt":
                    return query.filter(
                        getattr(self.get_secondary_model_alias(), self._column) > self._filter_value
                    )
                if self._operator == "lte":
                    return query.filter(
                        getattr(self.get_secondary_model_alias(), self._column) <= self._filter_value
                    )
                if self._operator == "lt":
                    return query.filter(
                        getattr(self.get_secondary_model_alias(), self._column) < self._filter_value
                    )
        self._operator = "eq"
        return query.filter(getattr(self.get_secondary_model_alias(), self._column) == self._filter_value)


class ManyToManyKeyValueJoinFilter(BaseJoinFilter):
    def __init__(self, model, filter_key, filter_value):
        super().__init__(model, filter_key, filter_value)
        self._key_field = None
        self._value_field = None

    def set_key_field(self, field):
        self._key_field = field
        return self

    def set_value_field(self, field):
        self._value_field = field
        return self

    def add_to_query(self, query):
        query = query.join(
            self.get_intermediate_model_alias(),
            getattr(self._model, self._model_to_intermediate_relation)
        )
        query = query.join(
            self.get_secondary_model_alias(),
            getattr(self.get_intermediate_model_alias(), self._intermediate_to_secondary_relation)
        )

        key_fields = self._filter_key.split("__")
        key_value = key_fields[1]
        self._column = key_fields[2]
        if len(key_fields) == 4:
            self._operator = key_fields[3]
            if self.is_valid_column(self._secondary_model):
                if self._operator == "in":
                    return query.filter(
                        getattr(self.get_intermediate_model_alias(), self._key_field) == key_value,
                        getattr(
                            self.get_secondary_model_alias(), self._column
                        ).in_(
                            self.get_list(self._filter_value)
                        )
                    )
                if self._operator == "exclude":
                    return query.filter(
                        getattr(self.get_intermediate_model_alias(), self._key_field) == key_value,
                        getattr(
                            self.get_secondary_model_alias(), self._column
                        ).notin_(
                            self.get_list(self._filter_value)
                        )
                    )
                if self._operator == "contains":
                    return query.filter(
                        getattr(self.get_intermediate_model_alias(), self._key_field) == key_value,
                        getattr(self.get_secondary_model_alias(), self._column).like(
                            "%%%s%%" % str(self._filter_value)
                        )
                    )
                if self._operator == "startswith":
                    return query.filter(
                        getattr(self.get_intermediate_model_alias(), self._key_field) == key_value,
                        getattr(self.get_secondary_model_alias(), self._column).like(
                            "%s%%" % str(self._filter_value)
                        )
                    )
                if self._operator == "endswith":
                    return query.filter(
                        getattr(self.get_intermediate_model_alias(), self._key_field) == key_value,
                        getattr(self.get_secondary_model_alias(), self._column).like(
                            "%%%s" % str(self._filter_value)
                        )
                    )
                if self._operator == "soundex":
                    return query.filter(
                        getattr(self.get_intermediate_model_alias(), self._key_field) == key_value,
                        getattr(self.get_secondary_model_alias(), self._column).op(
                            "SOUNDS LIKE"
                        )(str(self._filter_value))
                    )
                if self._operator == "gte":
                    return query.filter(
                        getattr(self.get_intermediate_model_alias(), self._key_field) == key_value,
                        getattr(self.get_secondary_model_alias(), self._column) >= self._filter_value
                    )
                if self._operator == "gt":
                    return query.filter(
                        getattr(self.get_intermediate_model_alias(), self._key_field) == key_value,
                        getattr(self.get_secondary_model_alias(), self._column) > self._filter_value
                    )
                if self._operator == "lte":
                    return query.filter(
                        getattr(self.get_intermediate_model_alias(), self._key_field) == key_value,
                        getattr(self.get_secondary_model_alias(), self._column) <= self._filter_value
                    )
                if self._operator == "lt":
                    return query.filter(
                        getattr(self.get_intermediate_model_alias(), self._key_field) == key_value,
                        getattr(self.get_secondary_model_alias(), self._column) < self._filter_value
                    )
        self._operator = "eq"
        return query.filter(
            getattr(self.get_intermediate_model_alias(), self._key_field) == key_value,
            getattr(self.get_secondary_model_alias(), self._column) == self._filter_value
        )


class KeyValueJoinFactory(BaseJoinFilter):
    def __init__(self, model, filter_key, filter_value):
        """I am very lazy!!!"""
        super().__init__(model, filter_key, filter_value)
        self._key_field = None
        self._value_field = None

    def set_key_field(self, field):
        self._key_field = field
        return self

    def set_value_field(self, field):
        self._value_field = field
        return self

    def add_to_query(self, query):
        """I am really lazy!!!"""
        pass

    def get(self):
        key_fields = self._filter_key.split("__")
        if len(key_fields) == 2:
            return OneToManyKeyValueJoinFilter(self._model, self._filter_key, self._filter_value).set_secondary_model(
                self._intermediate_model
            ).set_model_to_secondary_relation(
                self._model_to_intermediate_relation
            ).set_key_field(
                self._key_field
            ).set_value_field(
                self._value_field
            )
        elif len(key_fields) == 3 and \
                key_fields[2] in [
                    "in", "exclude", "contains", "startswith", "endswith", "soundex", "gte", "gt", "lte", "lt",
                ]:
            return OneToManyKeyValueJoinFilter(self._model, self._filter_key, self._filter_value).set_secondary_model(
                self._intermediate_model
            ).set_model_to_secondary_relation(
                self._model_to_intermediate_relation
            ).set_key_field(
                self._key_field
            ).set_value_field(
                self._value_field
            )
        return ManyToManyKeyValueJoinFilter(self._model, self._filter_key, self._filter_value).set_intermediate_model(
            self._intermediate_model
        ).set_model_to_intermediate_relation(
            self._model_to_intermediate_relation
        ).set_secondary_model(
            self._secondary_model
        ).set_intermediate_to_secondary_relation(
            self._intermediate_to_secondary_relation
        ).set_key_field(
            self._key_field
        ).set_value_field(
            self._value_field
        )
