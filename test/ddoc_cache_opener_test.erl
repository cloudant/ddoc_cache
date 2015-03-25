-module(ddoc_cache_opener_test).

-compile([export_all]).

-define(NODEBUG, true).
-define(FABRIC_DELAY, 100).
-define(DELAY, 1000).

-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

-record (cfg, {opener, stash}).
-record (ctx, {state = blocked, queue = [], rt = 0, rc = 0}).

main_test_() ->
    {spawn, {setup,
        fun setup/0,
        fun teardown/1,
        fun tests_builder/1
    }}.

setup() ->
    {ok, _SupPid} = ddoc_cache_fetcher_sup:start_link(),
    {ok, OpenerPid} = ddoc_cache_opener:start_link(),
    StashPid = spawn(?MODULE, stash, [#ctx{}]),
    meck:new([ets_lru, mem3, fabric]),
    %% blocking cache
    meck:expect(ets_lru, remove, fun(_, _) -> ok end),
    meck:expect(ets_lru, insert, fun(_, _, _) -> ok end),
    meck:expect(ets_lru, match, fun(_, _, _) ->
        Pid = self(),
        StashPid ! {q, Pid},
        receive {a, Pid} -> []
        after infinity -> ok
        end
    end),
    %% slow fabric
    meck:expect(fabric, open_doc, fun(_, Key, _) ->
        receive
        after ?FABRIC_DELAY ->
            {ok, #doc{id = Key, revs = {1, ["abc"]}}}
        end
    end),
    meck:expect(mem3, nodes, fun() -> [node()] end),
    #cfg{opener = OpenerPid, stash = StashPid}.

teardown(#cfg{stash = StashPid}) ->
    StashPid ! done,
    meck:unload([ets_lru, mem3, fabric]).

tests_builder(Cfg) ->
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

%% assertions

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
    after 1000 -> throw({stash, timeout})
    end.

assertResponseDelayed(#cfg{stash = StashPid}) ->
    StashPid ! {get_time, self()},
    receive
        {rtime, Time, _} ->
            ?debugVal(Time),
            Time < ?DELAY andalso Time < 2 * ?FABRIC_DELAY
    after 1000 -> throw({stash, timeout})
    end.

%% fixtures and helpers

mass_ask_for_ddoc(#cfg{stash = StashPid}) ->
    lists:foreach(fun(_) ->
        %% spawing because it is blocking
        spawn(fun() ->
            T = erlang:now(),
            {ok, _DDoc} = ddoc_cache_opener:open_doc(<<"one">>, <<"ddoc">>),
            StashPid ! {set_time, timer:now_diff(erlang:now(), T)}
        end)
    end, lists:seq(1, 100)).

flood_queue(#cfg{opener = OpenerPid}) ->
    lists:foreach(fun(_) ->
        ok = gen_server:cast(OpenerPid, {do_evict, <<"two">>})
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
