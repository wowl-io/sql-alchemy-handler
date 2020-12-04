from sqlalchemy import *
from sqlalchemy.orm import load_only
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.dialects.mysql import pymysql

import requests
import re


class QueryHandler(object):
    def __init__(self, db, model):
        self._db = db
        self._model = model
        self._filters = []
        self._base_count_query = None
        self._base_query = None
        self._fields = []
        self._hydrates = []
        self._response_key = None
        self._order_by = "id"
        self._order_dir = "asc"
        self._offset = 0
        self._limit = 30
        self._has_id = False
        self._app = None
        self._has_hydration = False
        self._is_soft_deleted = True
        self._primary_key = "id"

    def set_fields(self, fields):
        if (fields):
            self._fields = fields.split(",")
        return self

    def get_fields(self):
        return self._fields

    def set_hydrates(self, hydrates):
        if hydrates:
            self._hydrates = hydrates.split(",")
        return self

    def set_response_key(self, response_key):
        self._response_key = response_key
        return self

    def set_order_by(self, order_by):
        self._order_by = order_by
        return self

    def set_order_dir(self, order_dir):
        self._order_dir = order_dir
        return self

    def set_offset(self, offset):
        self._offset = offset
        return self

    def set_limit(self, limit):
        self._limit = limit
        return self

    def set_app(self, app):
        self._app = app
        return self

    def set_primary_key(self, primary_key):
        self._primary_key = primary_key
        return self

    def not_soft_deleted(self):
        self._is_soft_deleted = False
        return self

    def use_hydration(self):
        self._has_hydration = True
        return self

    def set_base_count_query(self, query):
        self._base_count_query = query
        return self

    def get_base_count_query(self):
        if not self._base_count_query:
            self._base_count_query = self._db.query(func.count(func.distinct(getattr(self._model, self._primary_key))))
            if self._is_soft_deleted:
                self._base_count_query = self._base_count_query.filter(getattr(self._model, "is_deleted") == 'N')
            for f in self._filters:
                if f.is_join_filter:
                    continue
                try:
                    self._base_count_query = f.add_to_query(self._base_count_query)
                except AttributeError:
                    pass
        return self._base_count_query

    def set_base_query(self, query):
        self._base_query = query
        return self

    def get_base_query(self):
        if not self._base_query:
            self._base_query = self._db.query(self._model)
            if self._fields:
                query_fields = list(set(self._fields + self._model.DEFAULT_FIELDS))
                query_fields = filter(lambda f: f not in getattr(self._model, "FOREIGN_KEY_FIELDS", []), query_fields)
                self._base_query = self._base_query.options(load_only(*query_fields))
            if self._is_soft_deleted:
                self._base_query = self._base_query.filter(getattr(self._model, "is_deleted") == 'N')
            for f in self._filters:
                if f.is_join_filter:
                    continue
                try:
                    self._base_query = f.add_to_query(self._base_query)
                    if f.get_column() == self._primary_key and f.get_operator() == "eq":
                        self._has_id = True
                except AttributeError:
                    pass
        return self._base_query

    def add_filter(self, filter):
        self._filters.append(filter)
        return self

    def get_count_query(self):
        query = self.get_base_count_query()
        for f in self._filters:
            if not f.is_join_filter:
                continue
            try:
                query = f.add_to_query(query)
            except AttributeError:
                pass
        return query

    def get_query(self):
        query = self.get_base_query()
        for f in self._filters:
            if not f.is_join_filter:
                continue
            try:
                query = f.add_to_query(query)
            except AttributeError:
                pass
        order_func = getattr(getattr(self._model, self._order_by), self._order_dir)
        query = query.order_by(order_func()).offset(self._offset).limit(self._limit)
        return query

    def get_count(self):
        return self.get_count_query().scalar()

    def get_results(self):
        results = self.get_query().distinct()
        if self._response_key:
            responses = {}
            for result in results:
                responses[getattr(result, self._response_key)] = responses.get(getattr(result, self._response_key), [])
                if self._has_hydration:
                    if self._app:
                        responses[getattr(result, self._response_key)].append(
                            result.to_dict(self._fields, self._hydrates, self._app)
                        )
                    else:
                        responses[getattr(result, self._response_key)].append(
                            result.to_dict(self._fields, self._hydrates)
                        )
                else:
                    responses[getattr(result, self._response_key)].append(result.to_dict(self._fields))
            return responses
        if self._has_hydration:
            if self._app:
                return [result.to_dict(self._fields, self._hydrates, self._app) for result in results]
            return [result.to_dict(self._fields, self._hydrates) for result in results]
        return [result.to_dict(self._fields) for result in results]

    def get_return_payload(self):
        count = self.get_count()
        results = self.get_results()
        if self._has_id:
            if count > 0:
                return results[0]
            raise NoResultFound("ID not found")
        return {
            "total_count": count,
            "records": results,
        }


class EsQueryHandler(QueryHandler):
    def __init__(self, db, model):
        super().__init__(db, model)
        self._es_host = None
        self._es_auth = None
        self._id_alias = "numeric_id"
        self._results = {}

    def set_es(self, host, auth):
        self._es_host = host
        self._es_auth = auth
        return self

    def set_id_alias(self, alias):
        self._id_alias = alias
        return self

    def normalize(self, schema, result):
        normalized = {}
        query_fields = []
        if self._fields:
            query_fields = list(set(self._fields + self._model.DEFAULT_FIELDS))
        for field_index, field_key in enumerate(schema):
            normal_key = "id" if field_key["name"] == self._id_alias else field_key["name"]
            if normal_key.endswith("_q"):
                continue
            if query_fields and normal_key not in query_fields:
                continue
            normalized[normal_key] = result[field_index]
        return normalized

    def get_results(self):
        if self._has_id:
            if self._results['total'] > 0:
                return self.normalize(self._results['schema'], self._results['datarows'][0])
            raise NoResultFound("ID not found")
        results = {
            "total_count": self._results['total'],
            "records": [self.normalize(self._results['schema'], r) for r in self._results['datarows']]
        }
        if self._response_key:
            grouped_results = {
                "total_count": self._results['total'],
                "records": {}
            }
            for record in results["records"]:
                try:
                    grouped_results["records"][record[self._response_key]].append(record)
                except KeyError:
                    grouped_results["records"][record[self._response_key]] = [record]
            return grouped_results
        return results

    def get_query_text(self):
        query_text = str(self.get_query().statement.compile(
            dialect=pymysql.dialect(),
            compile_kwargs={"literal_binds": True}
        ))
        return re.sub("^SELECT.*FROM", 'SELECT * FROM', query_text, flags=re.DOTALL).replace(
            ' LIKE ', '.keyword LIKE '
        ).replace('...', '`.`').replace('```', '')

    def get_return_payload(self):
        response = requests.post(
            "%s/_opendistro/_sql" % self._es_host,
            json={
                "query": self.get_query_text()
            },
            auth=self._es_auth
        )
        self._results = response.json()
        return self.get_results()
