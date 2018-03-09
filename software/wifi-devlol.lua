local module = {}
local disp = require("display")

local function wifi_wait_ip(callback, network)
  if wifi.sta.getip()== nil then
    print("IP unavailable, Waiting...")
    disp.message("Connecting...")
  else
    tmr.stop(1)
    disp.message("Connected to " .. network)
    print("ESP8266 mode is: " .. wifi.getmode())
    print("MAC address is: " .. wifi.ap.getmac())
    print("IP is "..wifi.sta.getip())
    if type(callback) == 'function' then
        callback()
    end
  end
end

local function wifi_start(list_aps, callback)
    if list_aps then
        for key,value in pairs(list_aps) do
            if config.SSID and config.SSID[key] then
                wifi.setmode(wifi.STATION);
                wifi.sta.config {ssid=key,pwd=config.SSID[key]}
                wifi.sta.connect()
                disp.message("... " .. key)

                tmr.alarm(1, 2500, 1, function() wifi_wait_ip(callback, key) end)
            end
        end
    else
        print("Error getting AP list")
    end
end

function module.start(callback)  
  disp.setup()
  disp.display():setFont(u8g.font_6x10)
  disp.message("Configuring wifi")
  print("Configuring Wifi ...")
  wifi.setmode(wifi.STATION);
  wifi.sta.getap(function(aps) wifi_start(aps, callback) end)

  -- reconnect
  wifi.eventmon.register(wifi.eventmon.STA_DISCONNECTED, function(previous_State)
    if(previous_State==wifi.STA_GOTIP) then
      print("Station lost connection with access point\n\tAttempting to reconnect...")
    else
      print("STATION_CONNECTING")
    end
  end)

end

return module  
