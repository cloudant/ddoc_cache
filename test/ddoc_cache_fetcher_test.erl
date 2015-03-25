-module(ddoc_cache_fetcher_test).

-compile([export_all]).

% -define(NODEBUG, true).
-define (FABRIC_DELAY, 200).

-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-record(cfg, {pid, doc, stash}).

ok_test_() ->
    {setup,
        fun ok_setup/0,
        fun ok_teardown/1,
        fun ok_run/1
    }.

batch_test_() ->
    {setup,
        fun batch_setup/0,
        fun batch_teardown/1,
        fun batch_run/1
    }.

fail_test_() ->
    {setup,
        fun fail_setup/0,
        fun fail_teardown/1,
        fun fail_run/1
    }.

common_setup() ->
    Doc = #doc{id = "doc", revs = {1, ["abc"]}},
    Modules = [ddoc_cache_fetcher_sup,ets_lru, mem3, fabric],
    ok = meck:new(Modules),
    true = meck:validate(Modules),
    meck:expect(ddoc_cache_fetcher_sup, set_revision, fun(_, _) -> ok end),
    meck:expect(ets_lru, remove, fun(_, _) -> ok end),
    meck:expect(ets_lru, insert, fun(_, _, _) -> ok end),
    meck:expect(ets_lru, match, fun(_, _, _) -> [] end),
    meck:expect(mem3, nodes, fun() -> [node()] end),
    meck:expect(ddoc_cache_fetcher_sup, start_child, fun({K, _, _}) ->
        {ok, K}
    end),
    {ok, Doc}.

ok_setup() ->
    {ok, Doc} = common_setup(),
    meck:expect(fabric, open_doc, fun(_, _, _) ->
        timer:sleep(?FABRIC_DELAY),
        {ok, Doc}
    end),
    meck:expect(fabric, open_revs, fun(_, _, _, _) ->
        timer:sleep(?FABRIC_DELAY),
        {ok, [{ok, Doc}]}
    end),
    % process_flag(trap_exit, true),
    {ok, Pid} = ddoc_cache_fetcher:start_link({"db", "doc", "1-abc"}),
    #cfg{pid = Pid, doc = Doc}.

batch_setup() ->
    {ok, Doc} = common_setup(),
    meck:expect(fabric, open_doc, fun(_, _, _) ->
        timer:sleep(?FABRIC_DELAY),
        {ok, Doc}
    end),
    meck:expect(fabric, open_revs, fun(_, _, _, _) ->
        timer:sleep(?FABRIC_DELAY),
        {ok, [{ok, Doc}]}
    end),
    % process_flag(trap_exit, true),
    {ok, Pid} = ddoc_cache_fetcher:start_link({"db", "doc", "1-abc"}),
    Cfg = #cfg{pid = Pid, doc = Doc},
    StashPid = spawn(?MODULE, stash, [Cfg, []]),
    Cfg#cfg{stash = StashPid}.

fail_setup() ->
    {ok, Doc} = common_setup(),
    meck:expect(ddoc_cache_fetcher_sup, terminate_child, fun(_) ->
        ok
    end),
    #cfg{doc = Doc}.

ok_teardown(_Cfg) ->
    Modules = [ddoc_cache_fetcher_sup, ets_lru, mem3, fabric],
    meck:unload(Modules).

batch_teardown(#cfg{stash = Pid} = Cfg) ->
    Pid ! done,
    ok_teardown(Cfg).

fail_teardown(Cfg) ->
    ok_teardown(Cfg).

ok_run(#cfg{pid = Pid, doc = Doc}) ->
    {Time, Rsp} = timer:tc(gen_server, call, [Pid, open]),
    [
        {"A call waited for a response", ?_assert(Time >= ?FABRIC_DELAY)},
        {"Got a proper response", ?_assertEqual(Rsp, {ok, Doc})},
        {"Server's done and gone", ?_test(fun() ->
            erlang:yield(),
            ?assertNot(is_process_alive(Pid))
        end)}
    ].

batch_run(#cfg{pid = Server, doc = Doc, stash = Pid}) ->
    Pid ! {call, 10},
    timer:sleep(2 * ?FABRIC_DELAY),
    Pid ! {result, self()},
    receive {ok, Acc} ->
    [
        {"Got all the expected responses", ?_assertEqual(length(Acc), 10)},
        {"All the responses are proper",
            ?_assert(lists:all(fun(D) -> D =:= Doc end, Acc))},
        {"Server's done and gone", ?_test(fun() ->
            erlang:yield(),
            ?assertNot(is_process_alive(Server))
        end)}
    ]   
    after 1000 ->
        throw(timeout)
    end.

fail_run(_Cfg) ->
    DoException = fun() ->
        meck:expect(fabric, open_doc, fun(_, _, _) ->
            timer:sleep(?FABRIC_DELAY),
            meck:exception(throw, fabric)
        end),
        {ok, Pid} = gen_server:start(ddoc_cache_fetcher, {"db", "doc"}, []),
        ?assert(is_process_alive(Pid)),
        ?assertExit({fabric,_}, gen_server:call(Pid, open)),
        ?assertNot(is_process_alive(Pid))
    end,
    DoExit = fun() ->
        meck:expect(fabric, open_doc, fun(_, _, _) ->
            timer:sleep(?FABRIC_DELAY),
            meck:exception(exit, fabric)
        end),
        {ok, Pid} = gen_server:start(ddoc_cache_fetcher, {"db", "doc"}, []),
        ?assert(is_process_alive(Pid)),
        ?assertExit({fabric,_}, gen_server:call(Pid, open)),
        ?assertNot(is_process_alive(Pid))
    end,
    DoError = fun() ->
        meck:expect(fabric, open_doc, fun(_, _, _) ->
            timer:sleep(?FABRIC_DELAY),
            meck:exception(error, fabric)
        end),
        {ok, Pid} = gen_server:start(ddoc_cache_fetcher, {"db", "doc"}, []),
        ?assert(is_process_alive(Pid)),
        ?assertExit({fabric,_}, gen_server:call(Pid, open)),
        ?assertNot(is_process_alive(Pid))
    end,
    [
        {"Exception from fabric call passed to fetcher",?_test(DoException())},
        {"Exit from fabric call passed to fetcher", ?_test(DoExit())},
        {"Error from fabric call passed to fetcher", ?_test(DoError())}
    ].

stash(#cfg{pid = Pid} = Cfg, Acc) ->
    receive
    {call, N} ->
        Me = self(),
        lists:foreach(fun(_) ->
            spawn(fun() -> Me ! gen_server:call(Pid, open) end)
        end, lists:seq(1,N)),
        stash(Cfg, Acc);
    {ok, Doc} ->
        stash(Cfg, [Doc|Acc]);
    {result, Caller} ->
        Caller ! {ok, Acc},
        stash(Cfg, Acc);
    done ->
        ok
    end.