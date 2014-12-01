% Copyright 2012 Cloudant. All rights reserved.

-module(ddoc_cache_sup).
-behaviour(supervisor).


-export([
    start_link/0,
    init/1
]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    Children = [
        {
            ddoc_cache_lru,
            {ets_lru, start_link, [ddoc_cache_lru, lru_opts()]},
            permanent,
            5000,
            worker,
            [ets_lru]
        },
        {
            ddoc_cache_fetcher_sup,
            {ddoc_cache_fetcher_sup, start_link, [ddoc_cache_fetcher]},
            permanent,
            5000,
            supervisor,
            [ddoc_cache_fetcher_sup]
        },
        {
            ddoc_cache_opener,
            {ddoc_cache_opener, start_link, []},
            permanent,
            5000,
            worker,
            [ddoc_cache_opener]
        }
    ],
    {ok, {{one_for_one, 5, 10}, Children}}.


lru_opts() ->
    lists:append([
        lru_opts(max_objects),
        lru_opts(max_size),
        lru_opts(max_lifetime)
    ]).

lru_opts(max_objects) ->
    lru_opts(max_objects, config:get("ddoc_cache", "max_objects", "-1"));
lru_opts(max_size) ->
    lru_opts(max_size, config:get("ddoc_cache", "max_size", "104857600"));
lru_opts(max_lifetime) ->
    lru_opts(max_lifetime, config:get("ddoc_cache", "max_lifetime", "-1")).

lru_opts(Key, String) ->
    case list_to_integer(String) of
    Num when Num > 0 -> [{Key, Num}];
    _ -> []
    end.
