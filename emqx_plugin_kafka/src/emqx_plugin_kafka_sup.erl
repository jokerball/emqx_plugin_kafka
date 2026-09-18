-module(emqx_plugin_kafka_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [
        #{
            id => emqx_plugin_kafka,
            start => {emqx_plugin_kafka, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_plugin_kafka]
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
