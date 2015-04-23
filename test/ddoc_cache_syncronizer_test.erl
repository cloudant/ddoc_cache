-module(ddoc_cache_syncronizer_test).


-compile([export_all]).


-define(NODEBUG, true).
-define(DELAY, 1000).
-define(CACHE, ddoc_cache_lru).


-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


-record(cfg, {sup, key, rev}).


ok_test_() ->
    {setup,
        fun setup/0,
        fun teardown/1,
        fun build_tests/1
    }.


setup() ->
    ets:new(?CACHE, [set, named_table, public]),
    Modules = [ddoc_cache_opener],
    ok = meck:new(Modules),
    true = meck:validate(Modules),
    meck:expect(ddoc_cache_opener, member, fun(_) ->
        true
    end),
    meck:expect(ddoc_cache_opener, evict_docs, fun(_, _) ->
        ok
    end),
    meck:expect(ddoc_cache_opener, recover_doc_info, fun(_, _) ->
        {ok, #doc_info{revs = [#rev_info{rev={1, a}}]}}
    end),
    process_flag(trap_exit, true),
    {ok, Pid} = ddoc_cache_fetcher_sup:start_link(),
    #cfg{sup = Pid, key = {a, a}, rev = {1, a}}.


teardown(#cfg{sup = Pid}) ->
    Modules = [ddoc_cache_opener],
    meck:unload(Modules),
    exit(Pid, kill),
    receive
        {'EXIT',Pid,killed} -> ok;
    % There's a missing after here right?
    ?DELAY ->
        exit(sup_timeout)
    end.


build_tests(Cfg) ->
    [
        {"Start syncronizer", ?_test(assertSyncStart(Cfg))},
        {"Refresh syncronizer", ?_test(assertSyncRefresh(Cfg))},
        {"Stop syncronizer", ?_test(assertSyncStop(Cfg))}
    ].


assertSyncStart(#cfg{key = Key, rev = Rev}) ->
    {ok, Pid} = ddoc_cache_synchronizer:start(Key, Rev),
    {_,Info} = erlang:process_info(Pid, current_function),
    ?debugVal(Info),
    is_pid(Pid) and is_process_alive(Pid) and (Info =:= {erlang,hibernate,3}).


assertSyncRefresh(#cfg{key = Key}) ->
    Rsp = ddoc_cache_synchronizer:refresh(Key),
    erlang:yield(),
    Rsp =:= ok.


assertSyncStop(#cfg{key = Key}) ->
    Rsp = ddoc_cache_synchronizer:stop(Key),
    erlang:yield(),
    Rsp =:= ok.
