-module(ddoc_cache_opener_test).

-compile([export_all]).


-define(NODEBUG, true).
-define(CACHE, ddoc_cache_lru).
-define(FABRIC_DELAY, 100).
-define(DELAY, 1000).


-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


-record (cfg, {sup, keeper, opener, stash}).
-record (ctx, {state = blocked, queue = [], rt = 0, rc = 0}).


cache_crud_test_() ->
    {spawn, {setup,
        fun cache_setup/0,
        fun cache_teardown/1,
        fun cache_tests_builder/1
    }}.


cache_setup() ->
    meck:new([config]),
    mock_config(0, 104857600),
    {ok, KeeperPid, SupPid, OpenerPid} = start_opener(),
    #cfg{sup = SupPid, keeper = KeeperPid, opener = OpenerPid}.


cache_teardown(#cfg{sup = SupPid, keeper = KeeperPid, opener = OpenerPid}) ->
    stop_opener(KeeperPid, SupPid, OpenerPid),
    meck:unload([config]).


cache_tests_builder(_Cfg) ->
    [
        {"cache exists", ?_test(assertCacheCreated())},
        {"store some docs", ?_test(assertCanStore())},
        {"lookup some docs", ?_test(assertCanLookup())},
        {"check some docs", ?_test(assertCanCheckExistance())},
        {"match some docs", ?_test(assertCanMatch())},
        {"remove a doc", ?_test(assertCanRemove())},
        {"remove matched docs", ?_test(assertCanMatchRemove())}
    ].


assertCacheCreated() ->
    ets:info(?CACHE, size) =:= 0.


assertCanStore() ->
    Keys = [
        {{a, a}, 1},
        {{a, custom, validation}, 2},
        {{a, a, {1, a}}, 3},
        {{a, a, {2, b}}, 4},
        {{a, a, {3, c}}, 5},
        {{a, b}, 6},
        {{a, b, {1, a}}, 7},
        {{a, b, {2, b}}, 8},
        {{a, b, {3, c}}, 9}
    ],
    Results = [ddoc_cache_opener:store_doc(K, V) || {K,V} <- Keys],
    (length(Results) =:= length(Keys))
        and lists:all(fun(R) -> R =:= ok end, Results).


assertCanLookup() ->
    A = ddoc_cache_opener:lookup({a, a}),
    B = ddoc_cache_opener:lookup({a, custom, validation}),
    C = ddoc_cache_opener:lookup({a, a, {3, c}}),
    D = ddoc_cache_opener:lookup({a, b}),
    E = ddoc_cache_opener:lookup({a, z}),
    {A, B, C, D, E} =:= {{ok, 1}, {ok, 2}, {ok, 5}, {ok, 6}, missing}.


assertCanCheckExistance() ->
    A = ddoc_cache_opener:member({a, a}),
    B = ddoc_cache_opener:member({a, z}),
    C = ddoc_cache_opener:member({b, a}),
    D = ddoc_cache_opener:member({a, a, {2, b}}),
    {A, B, C, D} =:= {true, false, false, true}.


assertCanMatch() ->
    A = ddoc_cache_opener:match({a, '_'}),
    B = ddoc_cache_opener:match({a, '_', '_'}),
    C = ddoc_cache_opener:match({a, custom, '_'}),
    (length(A) =:= 2) and (length(B) =:= 7) and (length(C) =:= 1).


assertCanRemove() ->
    A = ddoc_cache_opener:lookup({a, a, {1, a}}),
    B = ddoc_cache_opener:remove_doc({a, a, {1, a}}),
    C = ddoc_cache_opener:lookup({a, a, {1, a}}),
    {A, B, C} =:= {5, ok, missing}.


assertCanMatchRemove() ->
    A = ddoc_cache_opener:match({a, custom, '_'}),
    B = ddoc_cache_opener:remove_match_docs({a, custom, '_'}),
    C = ddoc_cache_opener:match({a, custom, '_'}),
    D = ddoc_cache_opener:match({a, '_', '_'}),
    E = ddoc_cache_opener:remove_doc({a, '_', '_'}),
    F = ddoc_cache_opener:match({a, '_', '_'}),
    ({length(A), B, C} =:= {1, ok, []})
        and ({length(D), E, F} =:= {5, ok, []}).


lru_test_() ->
    {spawn, {setup,
        fun lru_setup/0,
        fun lru_teardown/1,
        fun lru_tests_builder/1
    }}.


lru_setup() ->
    meck:new([config]),
    mock_config(10, 2000),
    {ok, KeeperPid, SupPid, OpenerPid} = start_opener(),
    #cfg{sup = SupPid, keeper = KeeperPid, opener = OpenerPid}.


lru_teardown(#cfg{sup = SupPid, keeper = KeeperPid, opener = OpenerPid}) ->
    stop_opener(KeeperPid, SupPid, OpenerPid),
    meck:unload([config]).


lru_tests_builder(_Cfg) ->
    [
        {"cache exists", ?_test(assertCacheCreated())},
        {"store 100 doc", ?_test(generate_ddocs(100))},
        {"doc count limited", ?_test(assertDocsCount())},
        {"cache size limited", ?_test(assertCacheSize())},
        {"recently touched kept", ?_test(assertRecentKept())}
    ].


assertDocsCount() ->
    Size = ets:info(?CACHE, size),
    ?debugVal(Size),
    Size =< 10.


assertCacheSize() ->
    Memory = ets:info(?CACHE, memory),
    ?debugVal(Memory),
    Memory =< 2000.


assertRecentKept() ->
    Result = lists:map(fun(I) ->
        case ddoc_cache_opener:lookup({a, I, {1, a}}) of
        {ok, _} -> true;
        missing -> false
        end
    end, lists:seq(1, 5)),
    lists:all(fun(True) -> True end, Result).


fetch_test_() ->
    {spawn, {setup,
        fun fetch_setup/0,
        fun fetch_teardown/1,
        fun fetch_tests_builder/1
    }}.


fetch_setup() ->
    {ok, KeeperPid, SupPid, OpenerPid} = start_opener(),
    StashPid = spawn(?MODULE, stash, [#ctx{}]),
    meck:new([mem3, fabric]),
    %% slow fabric
    meck:expect(fabric, open_doc, fun(_, Key, _) ->
        receive
        after ?FABRIC_DELAY ->
            {ok, #doc{id = Key, revs = {1, ["abc"]}}}
        end
    end),
    %% blocking mem3
    meck:expect(mem3, nodes, fun() ->
        Pid = self(),
        StashPid ! {q, Pid},
        receive
            {a, Pid} -> [node()]
        after
            infinity -> ok
        end
    end),
    #cfg{sup=SupPid, keeper=KeeperPid, opener=OpenerPid, stash=StashPid}.


fetch_teardown(#cfg{sup=Sup, keeper=Keeper, opener=Opener, stash=Stash}) ->
    stop_opener(Keeper, Sup, Opener),
    Stash ! done,
    meck:unload([mem3, fabric]).


fetch_tests_builder(Cfg) ->
    [
        {"ask 100 times for a slow ddoc", ?_test(mass_ask_for_ddoc(Cfg))},
        {"confirm opener's queue hasn't grow", ?_assert(assertQueueDry(Cfg))},
        {"confirm fetcher's queue hasn't grow",
            ?_assert(assertFetcherQueueDry())},
        {"flood ddoc_cache_opener queue", ?_test(flood_queue(Cfg))},
        {"confirm the queue flooded", ?_assert(assertQueueFlooded(Cfg))},
        {"wait longer than fabric's delay", ?_test(timer:sleep(?DELAY))},
        {"drain ddoc_cache_opener queue", ?_test(drain_queue(Cfg))},
        {"confirm opener's queue drined", ?_assert(assertQueueDry(Cfg))},
        {"confirm fetcher responded", ?_assert(assertResponsePassed(Cfg))},
        {"confirm there was no delay", ?_assert(assertResponseDelayed(Cfg))}
    ].


assertQueueFlooded(#cfg{opener = Pid}) ->
    {_, Q} = erlang:process_info(Pid, message_queue_len),
    ?debugVal(Q),
    Q >= 10.


assertQueueDry(#cfg{opener = Pid}) ->
    {_, Q} = erlang:process_info(Pid, message_queue_len),
    ?debugVal(Q),
    Q < 10.


assertFetcherQueueDry() ->
    Count = ddoc_cache_fetcher_sup:count_children(),
    ?debugVal(Count),
    ?assertEqual(Count, 1),
    %% get fetcher's pid
    Key = {<<"one">>, <<"ddoc">>},
    ChildSpec = {Key, ddoc_cache_fetcher, [Key]},
    {ok, Pid} = ddoc_cache_fetcher_sup:start_child(ChildSpec),
    ?debugVal(Pid),
    ?assert(is_process_alive(Pid)),
    {_, Q} = process_info(Pid, message_queue_len),
    ?debugVal(Q),
    Q =:= 0.


assertResponsePassed(#cfg{stash = StashPid}) ->
    StashPid ! {get_time, self()},
    receive
        {rtime, _, ClientsCount} ->
            ?debugVal(ClientsCount),
            ClientsCount =:= 100
    after
        1000 -> throw({stash, timeout})
    end.


assertResponseDelayed(#cfg{stash = StashPid}) ->
    StashPid ! {get_time, self()},
    receive
        {rtime, Time, _} ->
            ?debugVal(Time),
            Time < ?DELAY andalso Time < 2 * ?FABRIC_DELAY
    after
        1000 -> throw({stash, timeout})
    end.


mock_config(Count, Size) ->
    meck:expect(config, get_integer, fun
        (_, "max_objects", _) -> Count;
        (_, "max_size", _) -> Size
    end).


start_opener() ->
    process_flag(trap_exit, true),
    {ok, KeeperPid} = ddoc_cache_keeper:start_link(),
    {ok, SupPid} = ddoc_cache_fetcher_sup:start_link(),
    {ok, OpenerPid} = ddoc_cache_opener:start_link(),
    {ok, KeeperPid, SupPid, OpenerPid}.


stop_opener(KeeperPid, SupPid, OpenerPid) ->
    lists:foreach(fun(Pid) ->
        exit(Pid, shutdown),
        receive
            {'EXIT',Pid, shutdown} -> ok;
        ?DELAY ->
            exit({kill_timeout, Pid})
        end
    end, [OpenerPid, SupPid, KeeperPid]).


generate_ddocs(N) ->
    Keeper = fun() ->
        lists:foreach(fun(I) ->
            ddoc_cache_opener:lookup({a, I, {1, a}})
        end, lists:seq(1, 5))
    end,
    lists:foreach(fun(I) ->
        if I > 5 -> Keeper(); true -> ok end,
        K = {a, I, {1, a}},
        V = crypto:rand_bytes(1024),
        ok = ddoc_cache_opener:store_doc(K, V)
    end, lists:seq(1, N)),
    true.


mass_ask_for_ddoc(#cfg{stash = StashPid}) ->
    lists:foreach(fun(_) ->
        %% spawing because it is blocking
        spawn(fun() ->
            T = erlang:now(),
            {ok, _DDoc} = ddoc_cache_opener:open_doc(<<"one">>, <<"ddoc">>),
            StashPid ! {set_time, timer:now_diff(erlang:now(), T)}
        end)
    end, lists:seq(1, 100)),
    true.


flood_queue(#cfg{opener = OpenerPid}) ->
    lists:foreach(fun(_) ->
        ok = gen_server:cast(OpenerPid, {evict, <<"two">>})
    end, lists:seq(1,100)).


drain_queue(#cfg{stash = StashPid}) ->
    StashPid ! unblock,
    timer:sleep(5).


stash(#ctx{queue = Q} = Ctx) ->
    receive
        {q, Pid} when Ctx#ctx.state =:= blocked ->
            stash(Ctx#ctx{queue = [Pid|Q]});
        {q, Pid} ->
            Pid ! {a, Pid},
            stash(Ctx);
        {set_time, Time} ->
            RC = Ctx#ctx.rc + 1,
            RT = Ctx#ctx.rt + Time,
            stash(Ctx#ctx{rc = RC, rt = RT});
        {get_time, Pid} ->
            Pid ! {rtime, Ctx#ctx.rt / (Ctx#ctx.rc * 1000), Ctx#ctx.rc},
            stash(Ctx);
        unblock ->
            [Pid ! {a, Pid} || Pid <- Q],
            stash(Ctx#ctx{state = unblocked});
        done ->
            ok
    after infinity -> ok
    end.
