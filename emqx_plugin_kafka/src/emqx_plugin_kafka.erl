-module(emqx_plugin_kafka).

-define(PLUGIN_NAME, "emqx_plugin_kafka").
-define(PLUGIN_VSN, "1.0.0").

%% for #message{} record
-include_lib("emqx_plugin_helper/include/emqx.hrl").

%% for hook priority constants
-include_lib("emqx_plugin_helper/include/emqx_hooks.hrl").

%% for logging
-include_lib("emqx_plugin_helper/include/logger.hrl").

-export([
    hook/0,
    unhook/0,
    start_link/0
]).

-export([
    on_config_changed/2,
    on_health_check/1,
    get_config/0
]).

%% Hook callbacks
-export([
    on_client_connected/2,
    on_client_disconnected/3,
    on_message_publish/1,
    on_session_subscribed/3,
    on_message_delivered/2,
    on_message_acked/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% NOTE: Functions from EMQX are unavailable at compile time.
-dialyzer({no_unknown, [hook/0, unhook/0]}).

%%--------------------------------------------------------------------
%% Plugin lifecycle
%%--------------------------------------------------------------------

%% @doc Register hooks when plugin starts
hook() ->
    emqx_hooks:add('client.connected', {?MODULE, on_client_connected, []}, ?HP_HIGHEST),
    emqx_hooks:add('client.disconnected', {?MODULE, on_client_disconnected, []}, ?HP_HIGHEST),
    emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_HIGHEST),
    emqx_hooks:add('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_HIGHEST),
    emqx_hooks:add('message.delivered', {?MODULE, on_message_delivered, []}, ?HP_HIGHEST),
    emqx_hooks:add('message.acked', {?MODULE, on_message_acked, []}, ?HP_HIGHEST).

%% @doc Unregister hooks when plugin stops
unhook() ->
    emqx_hooks:del('client.connected', {?MODULE, on_client_connected}),
    emqx_hooks:del('client.disconnected', {?MODULE, on_client_disconnected}),
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('message.delivered', {?MODULE, on_message_delivered}),
    emqx_hooks:del('message.acked', {?MODULE, on_message_acked}).

%%--------------------------------------------------------------------
%% Hook callbacks - Client lifecycle
%%--------------------------------------------------------------------

%% @doc Client connected - send heartbeat (online)
%% NOTE: EMQX 5 removed 'client.heartbeat' hook.
%% We use client.connected (online) + client.disconnected (offline) instead.
on_client_connected(ClientInfo, ConnInfo) ->
    ClientId = maps:get(clientid, ClientInfo, <<"unknown">>),
    Protocol = maps:get(proto_name, ConnInfo, <<"MQTT">>),
    Keepalive = maps:get(keepalive, ConnInfo, 0),
    logger:warning("on_client_connected: clientid=~s protocol=~s keepalive=~p~n", [ClientId, Protocol, Keepalive]),
    ?SLOG(debug, #{msg => "kafka_client_connected", clientid => ClientId}),
    Now = now_mill_secs(),
    Payload = [
        {action, <<"heartbeat">>},
        {protocol, Protocol},
        {conn_type, <<"tcp">>},
        {device_id, ClientId},
        {keepalive, Keepalive},
        {ts, Now},
        {cluster_node, node()}
    ],
    produce_kafka_payload(ClientId, Payload, on_client_heartbeat),
    ok.

%% @doc Client disconnected - send heartbeat (offline with off=1)
on_client_disconnected(ClientInfo, _Reason, ConnInfo) ->
    ClientId = maps:get(clientid, ClientInfo, <<"unknown">>),
    Protocol = maps:get(proto_name, ConnInfo, <<"MQTT">>),
    Keepalive = maps:get(keepalive, ConnInfo, 0),
    logger:warning("on_client_disconnected: clientid=~s reason=~p~n", [ClientId, _Reason]),
    ?SLOG(debug, #{msg => "kafka_client_disconnected", clientid => ClientId}),
    Now = now_mill_secs(),
    Payload = [
        {action, <<"heartbeat">>},
        {protocol, Protocol},
        {conn_type, <<"tcp">>},
        {device_id, ClientId},
        {keepalive, Keepalive},
        {ts, Now},
        {off, 1},
        {cluster_node, node()}
    ],
    produce_kafka_payload(ClientId, Payload, on_client_heartbeat),
    ok.

%%--------------------------------------------------------------------
%% Hook callbacks - Message publish (core routing logic)
%%--------------------------------------------------------------------

%% @doc Message publish - forward to Kafka with topic routing + base64
%% This is a fold hook: return {ok, Message} to continue, stop to halt
on_message_publish(Message) ->
    MsgMap = emqx_message:to_map(Message),
    Topic = maps:get(topic, MsgMap, <<>>),
    case Topic of
        <<"$SYS/", _/binary>> ->
            {ok, Message};
        _ ->
            ClientId = maps:get(from, MsgMap, <<"unknown">>),
            logger:warning("on_message_publish: topic=~s clientid=~s~n", [Topic, ClientId]),
            case topic_in_ignored_list(Topic) of
                true ->
                    ?SLOG(debug, #{msg => "kafka_topic_ignored", topic => Topic, clientid => ClientId});
                false ->
                    Headers = maps:get(headers, MsgMap, #{}),
                    Username = maps:get(username, Headers, <<>>),
                    IpAddr = maps:get(peerhost, Headers, undefined),
                    Properties = maps:get(properties, Headers, #{}),
                    ContentType = maps:get('Content-Type', Properties, <<"application/text">>),
                    Payload = maps:get(payload, MsgMap, <<>>),
                    Content = transform_payload(Payload),
                    KafkaPayload = [
                        {action, message_publish},
                        {device_id, ClientId},
                        {ipaddress, iolist_to_binary(ntoa(IpAddr))},
                        {username, Username},
                        {topic, Topic},
                        {payload, Content},
                        {content_type, ContentType},
                        {cluster_node, node()},
                        {ts, maps:get(timestamp, MsgMap, erlang:system_time(millisecond))}
                    ],
                    KafkaTopic = get_target_kafka_topic(Topic),
                    produce_kafka_payload_by_kafka_topic(ClientId, KafkaPayload, KafkaTopic)
            end,
            {ok, Message}
    end.

%%--------------------------------------------------------------------
%% Hook callbacks - Message delivered
%%--------------------------------------------------------------------

on_message_delivered(_ClientInfo, Message) ->
    MsgMap = emqx_message:to_map(Message),
    Topic = maps:get(topic, MsgMap, <<>>),
    case Topic of
        <<"$SYS/", _/binary>> ->
            ok;
        _ ->
            ClientId = maps:get(clientid, _ClientInfo, <<"unknown">>),
            case topic_in_ignored_list(Topic) of
                true ->
                    ?SLOG(debug, #{msg => "kafka_delivered_ignored", topic => Topic});
                false ->
                    Payload = transform_payload(maps:get(payload, MsgMap, <<>>)),
                    Content = [
                        {action, <<"message_delivered">>},
                        {from, maps:get(from, MsgMap, <<>>)},
                        {to, ClientId},
                        {topic, Topic},
                        {payload, Payload},
                        {qos, maps:get(qos, MsgMap, 0)},
                        {cluster_node, node()},
                        {ts, maps:get(timestamp, MsgMap, erlang:system_time(millisecond))}
                    ],
                    produce_kafka_payload(ClientId, Content, on_message_delivered)
            end,
            ok
    end.

%%--------------------------------------------------------------------
%% Hook callbacks - Message acked
%%--------------------------------------------------------------------

on_message_acked(_ClientInfo, Message) ->
    MsgMap = emqx_message:to_map(Message),
    Topic = maps:get(topic, MsgMap, <<>>),
    case Topic of
        <<"$SYS/", _/binary>> ->
            ok;
        _ ->
            ClientId = maps:get(clientid, _ClientInfo, <<"unknown">>),
            case topic_in_ignored_list(Topic) of
                true ->
                    ?SLOG(debug, #{msg => "kafka_acked_ignored", topic => Topic});
                false ->
                    Payload = transform_payload(maps:get(payload, MsgMap, <<>>)),
                    Content = [
                        {action, <<"message_acked">>},
                        {from, maps:get(from, MsgMap, <<>>)},
                        {to, ClientId},
                        {topic, Topic},
                        {payload, Payload},
                        {qos, maps:get(qos, MsgMap, 0)},
                        {cluster_node, node()},
                        {ts, maps:get(timestamp, MsgMap, erlang:system_time(millisecond))}
                    ],
                    produce_kafka_payload(ClientId, Content, on_message_acked)
            end,
            ok
    end.

%%--------------------------------------------------------------------
%% Hook callbacks - Session subscribed
%%--------------------------------------------------------------------

on_session_subscribed(ClientInfo, Topic, SubOpts) ->
    ClientId = maps:get(clientid, ClientInfo, <<"unknown">>),
    Config = get_config(),
    case Config of
        #{<<"session_subscribed_topic">> := _KafkaTopic} ->
            Qos = maps:get(qos, SubOpts, 0),
            Payload = [
                {device_id, ClientId},
                {action, <<"subscribed">>},
                {topic, Topic},
                {qos, Qos},
                {ts, now_mill_secs()},
                {cluster_node, node()}
            ],
            produce_kafka_payload(ClientId, Payload, on_session_subscribed);
        _ ->
            ok
    end,
    ok.

%%--------------------------------------------------------------------
%% Topic routing: prefix -> exact -> default
%%--------------------------------------------------------------------

%% @doc Main routing: prefix match -> exact match -> default topic
get_target_kafka_topic(MqttTopic) ->
    case get_kafka_topic_by_prefix(MqttTopic) of
        KafkaTopic when is_binary(KafkaTopic) ->
            KafkaTopic;
        _ ->
            case get_kafka_topic_by_mqtt_topic(MqttTopic) of
                KafkaTopic2 when is_binary(KafkaTopic2) ->
                    KafkaTopic2;
                _ ->
                    get_default_kafka_topic()
            end
    end.

%% @doc Prefix matching: iterate configured prefix mappings
get_kafka_topic_by_prefix(MqttTopic) ->
    Config = get_config(),
    PrefixMappings = maps:get(<<"topic_prefix_mapping">>, Config, []),
    match_prefix(MqttTopic, PrefixMappings).

match_prefix(_MqttTopic, []) -> undefined;
match_prefix(MqttTopic, [#{<<"mqtt_prefix">> := Prefix, <<"kafka_topic">> := KafkaTopic} | Rest]) ->
    PrefixLen = byte_size(Prefix),
    case MqttTopic of
        <<Prefix:PrefixLen/binary, _/binary>> -> KafkaTopic;
        _ -> match_prefix(MqttTopic, Rest)
    end;
match_prefix(MqttTopic, [_ | Rest]) ->
    match_prefix(MqttTopic, Rest).

%% @doc Exact matching: lookup MQTT topic in mapping table
get_kafka_topic_by_mqtt_topic(MqttTopic) ->
    Config = get_config(),
    TopicMappings = maps:get(<<"topic_mapping">>, Config, []),
    find_exact_mapping(MqttTopic, TopicMappings).

find_exact_mapping(_MqttTopic, []) -> undefined;
find_exact_mapping(MqttTopic, [#{<<"mqtt_topic">> := MqttTopic, <<"kafka_topic">> := KafkaTopic} | _]) ->
    KafkaTopic;
find_exact_mapping(MqttTopic, [_ | Rest]) ->
    find_exact_mapping(MqttTopic, Rest).

%% @doc Default Kafka topic
get_default_kafka_topic() ->
    Config = get_config(),
    maps:get(<<"kafka_topic">>, Config, <<"mqtt-emqx-ignored">>).

%% @doc Get event-specific Kafka topic, fall back to default
get_kafka_topic_for_event(Event) ->
    Config = get_config(),
    %% Map event names to config keys
    ConfigKey = case Event of
        on_client_heartbeat -> <<"on_client_heartbeat">>;
        on_session_subscribed -> <<"session_subscribed_topic">>;
        _ -> <<"kafka_topic">>
    end,
    maps:get(ConfigKey, Config, get_default_kafka_topic()).

%%--------------------------------------------------------------------
%% Topic ignore list
%%--------------------------------------------------------------------

topic_in_ignored_list(Topic) ->
    Config = get_config(),
    IgnoreList = maps:get(<<"ignored_topic_prefixes">>, Config, []),
    lists:any(fun(Pattern) -> topic_pattern_match(Topic, Pattern) end, IgnoreList).

topic_pattern_match(Topic, Pattern) ->
    ByteSize = byte_size(Pattern),
    case Topic of
        <<Pattern:ByteSize/binary, _/binary>> -> true;
        _ -> false
    end.

%%--------------------------------------------------------------------
%% Payload transformation (base64)
%%--------------------------------------------------------------------

transform_payload(Payload) ->
    Config = get_config(),
    NeedBase64 = maps:get(<<"publish_base64">>, Config, true),
    case NeedBase64 of
        true ->
            list_to_binary(base64:encode_to_string(Payload));
        false ->
            Payload
    end.

%%--------------------------------------------------------------------
%% Kafka produce
%%--------------------------------------------------------------------

produce_kafka_payload(Key, Message, Event) ->
    Topic = get_kafka_topic_for_event(Event),
    produce_kafka_payload_by_kafka_topic(Key, Message, Topic).

produce_kafka_payload_by_kafka_topic(Key, Message, KafkaTopic) ->
    MessageBody = emqx_utils_json:encode(Message),
    Payload = iolist_to_binary(MessageBody),
    logger:warning("kafka_produce: topic=~s key=~s payload_size=~p~n", [KafkaTopic, Key, byte_size(Payload)]),
    try
        brod:produce_cb(emqx_repost_worker, KafkaTopic, hash, Key, Payload, fun(_, _) -> ok end),
        logger:warning("kafka_produce: success topic=~s~n", [KafkaTopic])
    catch
        _Class:_Reason:_Stack ->
            logger:error("kafka_produce: FAILED topic=~s key=~s reason=~p~n", [KafkaTopic, Key, _Reason]),
            ?SLOG(error, #{msg => "kafka_produce_failed", topic => KafkaTopic, key => Key})
    end,
    ok.

%%--------------------------------------------------------------------
%% Kafka initialization
%%--------------------------------------------------------------------

kafka_init(Config) ->
    logger:warning("kafka_init: starting with config: ~p~n", [Config]),
    ?SLOG(warning, #{msg => "kafka_plugin_init_start"}),
    BootstrapServersStr = maps:get(<<"bootstrap_servers">>, Config, <<"10.171.25.211:9092">>),
    DefaultTopic = maps:get(<<"kafka_topic">>, Config, <<"mqtt-emqx-ignored">>),
    AddressList = parse_bootstrap_servers(BootstrapServersStr),
    logger:warning("kafka_init: bootstrap servers: ~p~n", [AddressList]),
    ?SLOG(warning, #{msg => "kafka_address_list", addresses => AddressList}),
    KafkaConfig = build_kafka_config(Config),
    ?SLOG(warning, #{msg => "kafka_producer_config", config => KafkaConfig}),
    ?SLOG(warning, #{msg => "kafka_default_topic", topic => DefaultTopic}),
    logger:warning("kafka_init: starting brod application...~n"),
    {ok, _} = application:ensure_all_started(brod),
    logger:warning("kafka_init: brod started, creating client...~n"),
    ok = brod:start_client(AddressList, emqx_repost_worker, KafkaConfig),
    logger:warning("kafka_init: brod client created successfully~n"),
    ?SLOG(warning, #{msg => "kafka_plugin_init_success"}),
    ok.

%% @doc Parse "host1:9092,host2:9092" -> [{"host1", 9092}, {"host2", 9092}]
parse_bootstrap_servers(ServersBin) when is_binary(ServersBin) ->
    Parts = binary:split(ServersBin, <<",">>, [global, trim_all]),
    lists:map(fun(Part) ->
        Trimmed = string:trim(Part),
        [Host, PortBin] = binary:split(Trimmed, <<":">>),
        {binary_to_list(Host), binary_to_integer(PortBin)}
    end, Parts);
parse_bootstrap_servers(ServersStr) when is_list(ServersStr) ->
    parse_bootstrap_servers(list_to_binary(ServersStr)).

%% @doc Build brod producer config from HOCON config map
build_kafka_config(Config) ->
    [
        {ack_timeout, get_int(Config, <<"ack_timeout">>, 1000)},
        {required_acks, get_int(Config, <<"required_acks">>, 0)},
        {max_retries, get_int(Config, <<"max_retries">>, 0)},
        {reconnect_cool_down_seconds, get_int(Config, <<"reconnect_cool_down_seconds">>, 10)},
        {query_api_versions, true},
        {max_linger_ms, get_int(Config, <<"max_linger_ms">>, 5)},
        {max_linger_count, get_int(Config, <<"max_linger_count">>, 2000)},
        {partition_onwire_limit, get_int(Config, <<"partition_onwire_limit">>, 8)},
        {partition_buffer_limit, get_int(Config, <<"partition_buffer_limit">>, 4096)},
        {max_batch_size, get_int(Config, <<"max_batch_size">>, 10485760)},
        {auto_start_producers, true}
    ].

get_int(Config, Key, Default) ->
    case maps:get(Key, Config, Default) of
        V when is_integer(V) -> V;
        V when is_binary(V) -> binary_to_integer(V);
        _ -> Default
    end.

%%--------------------------------------------------------------------
%% Utility functions
%%--------------------------------------------------------------------

ntoa({0, 0, 0, 0, 0, 16#ffff, AB, CD}) ->
    inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(undefined) ->
    "";
ntoa(IP) when is_tuple(IP) ->
    case inet_parse:ntoa(IP) of
        {error, _} -> "";
        Res -> Res
    end;
ntoa(_) ->
    "".

now_mill_secs() ->
    erlang:system_time(millisecond).

%%--------------------------------------------------------------------
%% Plugin config callbacks
%%--------------------------------------------------------------------

on_health_check(_Options) ->
    ok.

on_config_changed(_OldConfig, NewConfig) ->
    %% Validate bootstrap_servers exists
    case maps:get(<<"bootstrap_servers">>, NewConfig, undefined) of
        undefined ->
            {error, <<"Missing bootstrap_servers">>};
        _ ->
            ok = gen_server:cast(?MODULE, {on_changed, NewConfig}),
            ok
    end.

%%--------------------------------------------------------------------
%% Config access
%%--------------------------------------------------------------------

get_config() ->
    persistent_term:get(?MODULE, #{}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(trap_exit, true),
    logger:warning("emqx_plugin_kafka gen_server init started~n"),
    PluginNameVsn = <<?PLUGIN_NAME, "-", ?PLUGIN_VSN>>,
    try
        Config = emqx_plugin_helper:get_config(PluginNameVsn),
        case map_size(Config) of
            0 ->
                logger:warning("emqx_plugin_kafka: config is EMPTY~n"
                               "  Please ensure config file exists at:~n"
                               "  /opt/emqx/data/configs/emqx_plugin_kafka-1.0.0/config.hocon~n");
            _ ->
                logger:warning("emqx_plugin_kafka: config loaded, keys=~p~n", [maps:keys(Config)])
        end,
        ?SLOG(warning, #{msg => "kafka_plugin_gen_server_init", config => Config}),
        persistent_term:put(?MODULE, Config),
        %% Initialize Kafka client
        try
            kafka_init(Config)
        catch
            Class:Reason:Stack ->
                ?SLOG(error, #{msg => "kafka_init_failed", class => Class, reason => Reason, stack => Stack}),
                logger:error("kafka_init failed: ~p:~p~nstack: ~p~n", [Class, Reason, Stack])
        end,
        {ok, #{config => Config}}
    catch
        InitClass:InitReason:InitStack ->
            logger:error("emqx_plugin_kafka init crashed: ~p:~p~nstack: ~p~n", [InitClass, InitReason, InitStack]),
            {stop, {init_failed, InitClass, InitReason}}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({on_changed, NewConfig}, State) ->
    persistent_term:put(?MODULE, NewConfig),
    %% Reinitialize Kafka with new config
    try
        brod:stop_client(emqx_repost_worker)
    catch _:_ -> ok
    end,
    try
        kafka_init(NewConfig)
    catch
        Class:Reason:Stack ->
            ?SLOG(error, #{msg => "kafka_reinit_failed", class => Class, reason => Reason, stack => Stack})
    end,
    {noreply, State#{config => NewConfig}};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    try
        brod:stop_client(emqx_repost_worker)
    catch _:_ -> ok
    end,
    persistent_term:erase(?MODULE),
    ok.
