-module(ddoc_cache_fetcher_sup_test).

-compile([export_all]).
-export([init/1]).
-behaviour(supervisor).

-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

default_test_() ->
    {setup,
        fun setup/0,
        fun teardown/1,
        fun test_run/1
    }.

setup() ->
    Pid = spawn(fun() ->
        process_flag(trap_exit, true),
        {ok, SupPid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
        receive _ -> {ok, SupPid}
        after infinity -> ok
        end
    end),
    timer:sleep(50), %% give sup a moment to start
    Pid.

init([]) ->
    Children = [{dummy_sup, {ddoc_cache_fetcher_sup, start_link, [dummy]},
        transient, 50, supervisor, [ddoc_cache_fetcher_sup]}],
    {ok, {{one_for_one, 1, 5}, Children}}.

teardown(_Pid) ->
    ok.

test_run(Pid) ->
    [
        {"Supervisor alive",
            ?_assert(is_process_alive(whereis(ddoc_cache_fetcher_sup)))},
        {"Can call count with no children",
            ?_assertEqual(0, ddoc_cache_fetcher_sup:count_children())},
        {"Can start a child", ?_test(start_child())},
        {"Can cound the child",
            ?_assertEqual(1, ddoc_cache_fetcher_sup:count_children())},
        {"Can find the child", ?_test(find_child())},
        {"Handle finished children", ?_test(quit_child())},
        {"Can start a child again", ?_test(start_child())},
        {"Can terminate a child", ?_test(terminate_child())},
        {"Handle exception in child", ?_test(explode_child())},
        {"Can start number of children", ?_test(start_children())},
        {"Can fetch list of children", ?_test(which_children())},
        {"Can clean shutdown supervisor", ?_test(shutdown_sup(Pid))}
    ].

start_child() ->
    {ok, Pid} = ddoc_cache_fetcher_sup:start_child(dummy),
    RegPid = whereis(dummy),
    ?assertEqual(Pid, RegPid),
    ?assert(is_process_alive(Pid)).

find_child() ->
    RegPid = whereis(dummy),
    {ok, Pid} = ddoc_cache_fetcher_sup:start_child(dummy),
    ?assertEqual(Pid, RegPid),
    ?assertEqual(1, ddoc_cache_fetcher_sup:count_children()).

quit_child() ->
    Pid = whereis(dummy),
    Ref = erlang:monitor(process, Pid),
    gen_server:call(dummy, {delay, 0}),
    receive {'DOWN',Ref,process,_,Reason} -> ?assertEqual(Reason, normal)
    end,
    erlang:demonitor(Ref, [flush]),
    erlang:yield(), %% give ets a chance
    ?assertNot(is_process_alive(Pid)),
    ?assertEqual(0, ddoc_cache_fetcher_sup:count_children()).

terminate_child() ->
    Pid = whereis(dummy),
    Ref = erlang:monitor(process, Pid),
    ok = ddoc_cache_fetcher_sup:terminate_child(dummy),
    receive {'DOWN',Ref,process,_,Reason} -> ?assertEqual(Reason, shutdown)
    end,
    erlang:demonitor(Ref, [flush]),
    erlang:yield(),
    ?assertNot(is_process_alive(Pid)),
    ?assertEqual(0, ddoc_cache_fetcher_sup:count_children()).

explode_child() ->
    start_child(),
    Pid = whereis(dummy),
    ?assertExit({{crowbar, _}, _}, gen_server:call(dummy, crash)),
    timer:sleep(5),
    ?assertNot(is_process_alive(Pid)),
    ?assertEqual(0, ddoc_cache_fetcher_sup:count_children()).

start_children() ->
    lists:foreach(fun(I) ->
        N = list_to_atom("dummy" ++ integer_to_list(I)),
        {ok, Pid} = ddoc_cache_fetcher_sup:start_child(N),
        ?assert(is_pid(Pid))
    end, lists:seq(1,9)),
    ?assertEqual(9, ddoc_cache_fetcher_sup:count_children()).

which_children() ->
    List = ddoc_cache_fetcher_sup:which_children(),
    lists:foldl(fun({Id,Pid}, I) ->
        N = list_to_atom("dummy" ++ integer_to_list(I)),
        ?assertEqual(N, Id),
        ?assert(is_pid(Pid)),
        ?assert(is_process_alive(Pid)),
        I + 1
    end, 1, lists:sort(List)).

shutdown_sup(Pid) ->
    Parent = whereis(?MODULE),
    SupPid = whereis(ddoc_cache_fetcher_sup),
    List = ddoc_cache_fetcher_sup:which_children(),
    ?assert(is_process_alive(Parent)),
    ?assert(is_process_alive(SupPid)),
    Ref = erlang:monitor(process, SupPid),
    Pid ! stop,
    receive {'DOWN',Ref,process,_,Reason} -> ?assertEqual(Reason, shutdown)
    end,
    erlang:demonitor(Ref, [flush]),
    timer:sleep(5),
    ?assertNot(is_process_alive(Parent)),
    ?assertNot(is_process_alive(SupPid)),
    lists:foreach(fun({_, P}) ->
        ?assertNot(is_process_alive(P))
    end, List).
