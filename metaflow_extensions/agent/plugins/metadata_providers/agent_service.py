"""
AgentServiceProvider -- extends ServiceMetadataProvider with paginated
and filtered fetching for agent workloads.

Falls back to the parent's unbounded behavior when the metadata service
doesn't support pagination (version < 2.3.0).
"""

import os

from metaflow.plugins.metadata_providers.service import (
    ServiceMetadataProvider,
    ServiceException,
)
from metaflow.metadata_provider import MetadataProvider


class AgentServiceProvider(ServiceMetadataProvider):
    TYPE = "agent_service"

    _http_calls = 0

    @classmethod
    def reset_call_count(cls):
        cls._http_calls = 0

    @classmethod
    def get_call_count(cls):
        return cls._http_calls

    @classmethod
    def _request(cls, monitor, path, method, data=None,
                 retry_409_path=None, return_raw_resp=False):
        cls._http_calls += 1
        return super()._request(
            monitor, path, method, data=data,
            retry_409_path=retry_409_path,
            return_raw_resp=return_raw_resp,
        )

    @classmethod
    def _get_object_internal(
        cls, obj_type, obj_order, sub_type, sub_order, filters, attempt, *args
    ):
        # for single-object fetches, just use the parent
        if sub_type == "self":
            return super()._get_object_internal(
                obj_type, obj_order, sub_type, sub_order,
                filters, attempt, *args,
            )

        # build url the same way the parent does
        if obj_type != "root":
            url = ServiceMetadataProvider._obj_path(*args[:obj_order])
        else:
            url = ""

        if sub_type == "metadata":
            url += "/metadata"
        elif sub_type == "artifact" and obj_type == "task" and attempt is not None:
            url += "/attempt/%s/artifacts" % attempt
        else:
            url += "/%ss" % sub_type

        # try paginated path if server supports it
        try:
            if hasattr(cls, '_fetch_paginated'):
                v = cls._fetch_paginated(url)
            else:
                v, _ = cls._request(None, url, "GET")
        except ServiceException as ex:
            if ex.http_code == 404:
                return None
            raise

        # push tag filters server-side when possible
        tag_filters = {}
        other_filters = {}
        if filters:
            for k, val in filters.items():
                if k in ("any_tags", "tags", "system_tags"):
                    tag_filters[k] = val
                else:
                    other_filters[k] = val

        # if we have tag filters and the server might not have handled them,
        # fall back to client-side filtering
        if other_filters:
            v = MetadataProvider._apply_filter(v, other_filters)

        return v
