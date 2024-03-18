-- DT LuaSDK
function script_path()
    local str = debug.getinfo(2, "S").source:sub(2)
    return str:match("(.*/)")
end
package.cpath = package.cpath .. ";" .. script_path() .. "?.so"
local dt_base = require("dt_core_lua")

local socket = require("socket")
local Util = {}
local DTLog = {}

local function class(base, _ctor)
    local c = {}
    if not _ctor and type(base) == 'function' then
        _ctor = base
        base = nil
    elseif type(base) == 'table' then
        for i, v in pairs(base) do
            c[i] = v
        end
        c._base = base
    end
    c.__index = c
    local mt = {}
    mt.__call = function(_, ...)
        local obj = {}
        setmetatable(obj, c)
        if _ctor then
            _ctor(obj, ...)
        end
        return obj
    end
    c._ctor = _ctor
    c.is_a = function(self, klass)
        local m = getmetatable(self)
        while m do
            if m == klass then
                return true
            end
            m = m._base
        end
        return false
    end
    setmetatable(c, mt)
    return c
end

local function divide(properties)
    local presetProperties = {}
    local finalProperties = {}
    for key, value in pairs(properties) do
        if (key == "#android_id" or key == "#event_syn" or key == "#bundle_id" or key == "#event_time"
                or key == "#app_id" or key == "#gaid" or key == "#dt_id" or key == "#acid"
                or key == "#event_name" or key == "#event_type"
        ) then
            presetProperties[key] = value
        else
            finalProperties[key] = value
        end
    end
    if (presetProperties["#event_syn"] == nil) then
        presetProperties["#event_syn"] = Util.create_uuid()
    end
    return finalProperties, presetProperties
end

---
---@param dtId string
---@param acId string
---@param eventType string
---@param eventName string
---@param properties table
---@param superProperties table
---@param dynamicSuperPropertiesTracker function
local function upload(dtId, acId, eventType, eventName, properties, superProperties, dynamicSuperPropertiesTracker)
    local finalProperties, presetProperties = divide(properties)
    local dynamicSuperProperties = {}
    if dynamicSuperPropertiesTracker ~= nil and type(dynamicSuperPropertiesTracker) == "function" then
        dynamicSuperProperties = dynamicSuperPropertiesTracker()
    end
    local eventJson = {}
    if acId ~= nil and string.len(acId) ~= 0 then
        eventJson["#acid"] = tostring(acId)
    end
    if dtId ~= nil and string.len(dtId) ~= 0 then
        eventJson["#dt_id"] = tostring(dtId)
    end
    eventJson["#event_type"] = eventType
    if eventName ~= nil and string.len(eventName) ~= 0 then
        eventJson["#event_name"] = tostring(eventName)
    end
    -- preset properties
    for key, value in pairs(presetProperties) do
        eventJson[key] = value
    end
    if presetProperties["#event_time"] == nil then
        local millTime = socket.gettime()
        eventJson["#event_time"] = math.floor(millTime * 1000)
    end
    local mergeProperties = {}
    if eventType == "track" then
        mergeProperties = Util.mergeTables(mergeProperties, superProperties)
        mergeProperties = Util.mergeTables(mergeProperties, dynamicSuperProperties)
    end
    mergeProperties["#sdk_type"] = DTAnalytics.platForm
    mergeProperties["#sdk_version_name"] = DTAnalytics.version
    mergeProperties = Util.mergeTables(mergeProperties, finalProperties)
    eventJson["properties"] = mergeProperties

    local ret = 0;
    ret = dt_base.add_event(eventJson)
    presetProperties = nil
    finalProperties = nil
    mergeProperties = nil
    eventJson = nil
    return ret
end

---
--- Init analytics instance
---@param self any
---@param consumer any consumer
DTAnalytics = class(function(self, consumer)
    if consumer == nil or type(consumer) ~= "table" or consumer.consumerProps == nil then
        DTLog.error("consumer params is invalidate.")
        return
    end
    self.superProperties = {}
    self.dynamicSuperPropertiesTracker = nil
    dt_base.init(consumer.consumerProps)

    DTLog.info("SDK init success")
end)

--- Enable log or not
---@param enable boolean
function DTAnalytics.enableLog(enable)
    DTLog.enable = enable
    dt_base.enable_log(enable)
end

--- Set common properties
---@param params table
function DTAnalytics:setSuperProperties(params)
    if (type(params) == "table") then
        self.superProperties = Util.mergeTables(self.superProperties, params)
    end
end

--- Set common property
---@param key string
---@param value any
function DTAnalytics:setSuperProperty(key, value)
    if (key ~= nil) then
        local params = {}
        params[key] = value
        DTLog.info(params[key])
        self:setSuperProperties(params)
    end
end

--- Remove common properties with key
---@param key any
function DTAnalytics:removeSuperProperty(key)
    if key == nil then
        return nil
    end
    self.superProperties[key] = nil
end

--- Find common properties with key
---@param key string
function DTAnalytics:getSuperProperty(key)
    if key == nil then
        return nil
    end
    return self.superProperties[key]
end

--- Get all properties
---@return table
function DTAnalytics:getSuperProperties()
    return self.superProperties
end

--- Clear common properties
function DTAnalytics:clearSuperProperties()
    self.superProperties = {}
end

--- Set user properties. Would overwrite existing names
---@param acId string
---@param dtId string
---@param properties table
function DTAnalytics:userSet(acId, dtId, properties)
    local ok, ret = pcall(upload, dtId, acId, "user", "#user_set", properties)
    if ok then
        return ret
    end
end

--- Set user properties, if such property had been set before, this message would be neglected.
---@param acId string
---@param dtId string
---@param properties table
function DTAnalytics:userSetOnce(acId, dtId, properties)
    local ok, ret = pcall(upload, dtId, acId, "user", "#user_set_once", properties)
    if ok then
        return ret
    end
end

--- To accumulate operations against the property
---@param acId string
---@param dtId string
---@param properties table
function DTAnalytics:userAdd(acId, dtId, properties)
    local ok, ret = pcall(upload, dtId, acId, "user", "#user_add", properties)
    if ok then
        return ret
    end
end

--- To add user properties of array type
---@param acId string
---@param dtId string
---@param properties table
function DTAnalytics:userAppend(acId, dtId, properties)
    local ok, ret = pcall(upload, dtId, acId, "user", "#user_append", properties)
    if ok then
        return ret
    end
end

--- Append user properties to array type by unique.
---@param acId string
---@param dtId string
---@param properties table
function DTAnalytics:userUniqAppend(acId, dtId, properties)
    local ok, ret = pcall(upload, dtId, acId, "user", "#user_uniq_append", properties)
    if ok then
        return ret
    end
end

--- Clear the user properties of users
---@param acId string
---@param dtId string
---@param properties table
function DTAnalytics:userUnset(acId, dtId, properties)
    local unSetProperties = {}
    for key, value in pairs(properties) do
        if Util.startWith(key, '#')then
            unSetProperties[key] = value
        else
            unSetProperties[key] = 0
        end
    end
    local ok, ret = pcall(upload, dtId, acId, "user", "#user_unset", unSetProperties)
    if ok then
        return ret
    end
end

--- Delete a user, This operation cannot be undone
---@param acId string
---@param dtId string
function DTAnalytics:userDelete(acId, dtId, properties)
    local ok, ret = pcall(upload, dtId, acId, "user", "#user_delete", properties)
    if ok then
        return ret
    end
end

--- Report ordinary event
---@param acId string
---@param dtId string
---@param eventName string
---@param properties table
function DTAnalytics:track(acId, dtId, eventName, properties)
    local ok, ret = pcall(upload, dtId, acId, "track", eventName, properties, self.superProperties, self.dynamicSuperPropertiesTracker)
    if ok then
        return ret
    end
end

--- Flush data
function DTAnalytics:flush()
    dt_base.flush()
end

--- Close SDK
function DTAnalytics:close()
    dt_base.close()
    DTLog.info("SDK closed!")
end


--- Construct LogConsumer
---@param self any
---@param logPath string
---@param batchNum number
---@param fileSize number
---@param fileNamePrefix string
DTAnalytics.DTLogConsumer = class(function(self, logPath, batchNum, fileSize, fileNamePrefix)
    self.consumerProps = {
        ["path"] = logPath,
        ["max_batch_len"] = batchNum,
        ["name_prefix"] = fileNamePrefix,
        ["max_file_size_bytes"] = fileSize
    }
end)

--- Set dynamic common properties
---@param callback function
function DTAnalytics:setDynamicSuperProperties(callback)
    if callback ~= nil then
        self.dynamicSuperPropertiesTracker = callback
    end
end


DTAnalytics.platForm = "dt_lua_sdk"
DTAnalytics.version = "1.0.0"

function Util.mergeTables(...)
    local tabs = { ... }
    if not tabs then
        return {}
    end
    local origin = tabs[1]
    for i = 2, #tabs do
        if origin then
            if tabs[i] then
                for k, v in pairs(tabs[i]) do
                    if (v ~= nil) then
                        origin[k] = v
                    end
                end
            end
        else
            origin = tabs[i]
        end
    end
    return origin
end

function Util.startWith(str, substr)
    if str == nil or substr == nil then
        return nil, "the string or the substring parameter is nil"
    end
    if string.find(str, substr) ~= 1 then
        return false
    else
        return true
    end
end

function Util.create_uuid()
    local uuidLib = require("uuid")
    return uuidLib()
end

Util.enableLog = false
DTLog.enable = false
function DTLog.info(...)
    if DTLog.enable then
        io.write("[DT Lua][" .. os.date("%Y-%m-%d %H:%M:%S") .. "][Info] ")
        print(...)
    end
end

function DTLog.error(...)
    if DTLog.enable then
        io.write("[DT Lua][" .. os.date("%Y-%m-%d %H:%M:%S") .. "][Error] ")
        print(...)
    end
end

return DTAnalytics
