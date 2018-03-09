
local module = {}

local client = nil
local options = nil


function module.setup(opts, connected)
    module.options = opts
    print ("mqtt-client setup")

    client = mqtt.Client(module.options.id, 12, module.options.user, module.options.password)
    client:lwt(module.options.endpoint .. "status", "offline", 0, 1)
    client:on("connect", function (client) 
	    print ("connected to mqtt " .. module.options.host) 
	    client:publish(module.options.endpoint .. "status", "online", 0, 1, function(client) print("sent 'online' to " .. module.options.endpoint .. "status" ) end)
	    if type(connected) == 'function' then
	       connected()
	    end
            end)
    client:on("offline", function (client)
	    print ("mqtt offline")
	    do_mqtt_connect()
    end)
    do_mqtt_connect()
    return module
end

function handle_mqtt_error(client, reason)
    print ("mqtt connection error: " .. reason .. " , reconnecting")
    tmr.create():alarm(10 * 1000, tmr.ALARM_SINGLE, do_mqtt_connect)
end

function handle_mqtt_connection(client)
    print ("connected to mqtt " .. module.options.host) 
    client:publish(module.options.endpoint .. "status", "online", 0, 1, function(client) print("sent 'online' to " .. module.options.endpoint .. "status" ) end)
end

function do_mqtt_connect()
    print ("connecting to mqtt " .. module.options.host)
    client:connect(module.options.host, module.options.port, handle_mqtt_connection, handle_mqtt_error)  
end


function module.client()
    return client
end

function module.message(topic, message, retain)
    client:publish(module.options.endpoint .. topic, message, 0, retain, function(client) print ("published " ..message .. " on " .. topic) end)
end

function module.read_topic(topic, callback)
    local topic = module.options.endpoint .. topic
    print ("reading old count from mqtt-topic " .. topic .. "if possible")

    local readtimeout = tmr.create()
    tmr.register(readtimeout, 5000, tmr.ALARM_SINGLE, function (t) print("expired"); callback (topic,0) tmr.unregister(t) end)
    tmr.start(readtimeout)

    print("subscribing to " .. topic)
    client:on('message', function(c, t, message)
        if t == topic then
            client:unsubscribe(topic, function() print("unsubscribed " .. topic) end)
	    tmr.unregister(readtimeout)
            callback(t, message)
        end
    end)
    client:subscribe(topic, 0)
   end

return module
