from abc import ABC, abstractmethod
from sqlalchemy import *
from urllib.parse import unquote


class BaseQueryFilter(ABC):
    is_join_filter = False

    def __init__(self, db, model, filter_key, filter_value):
        self._db = db
        self._model = model
        self._filter_key = filter_key
        self._filter_value = filter_value
        self._column = None
        self._operator = None

    def is_valid_column(self):
        return hasattr(self._model, self._column)

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
            if self.is_valid_column():
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
