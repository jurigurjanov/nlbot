from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable


_METHOD_SIGNATURE_CACHE_ATTR = "_xtb_method_signature_cache"


@dataclass(frozen=True, slots=True)
class CallableKeywordSupport:
    keyword_params: frozenset[str]
    accepts_var_keyword: bool
    inspect_failed: bool = False

    def filter_kwargs(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        if self.inspect_failed:
            return {}
        if self.accepts_var_keyword:
            return dict(kwargs)
        return {name: value for name, value in kwargs.items() if name in self.keyword_params}


def _callable_identity(getter: Callable[..., Any]) -> object:
    return getattr(getter, "__func__", getter)


def resolve_callable_keyword_support(getter: Callable[..., Any]) -> CallableKeywordSupport:
    try:
        signature = inspect.signature(getter)
    except (TypeError, ValueError):
        return CallableKeywordSupport(
            keyword_params=frozenset(),
            accepts_var_keyword=False,
            inspect_failed=True,
        )

    keyword_params: set[str] = set()
    accepts_var_keyword = False
    for name, parameter in signature.parameters.items():
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            accepts_var_keyword = True
            continue
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            keyword_params.add(name)

    return CallableKeywordSupport(
        keyword_params=frozenset(keyword_params),
        accepts_var_keyword=accepts_var_keyword,
        inspect_failed=False,
    )


def get_cached_method_keyword_support(
    owner: object | None,
    method_name: str,
    getter: Callable[..., Any],
) -> CallableKeywordSupport:
    if owner is None:
        return resolve_callable_keyword_support(getter)

    cache = getattr(owner, _METHOD_SIGNATURE_CACHE_ATTR, None)
    if not isinstance(cache, dict):
        cache = {}
        try:
            setattr(owner, _METHOD_SIGNATURE_CACHE_ATTR, cache)
        except Exception:
            return resolve_callable_keyword_support(getter)

    getter_identity = _callable_identity(getter)
    cached = cache.get(method_name)
    if (
        isinstance(cached, tuple)
        and len(cached) == 2
        and cached[0] is getter_identity
        and isinstance(cached[1], CallableKeywordSupport)
    ):
        return cached[1]

    support = resolve_callable_keyword_support(getter)
    cache[method_name] = (getter_identity, support)
    return support


def call_broker_method_with_supported_kwargs(
    owner: object | None,
    method_name: str,
    getter: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    support = get_cached_method_keyword_support(owner, method_name, getter)
    filtered_kwargs = support.filter_kwargs(kwargs)
    return getter(*args, **filtered_kwargs)
