-- DT LuaSDK
function script_path()
    local str = debug.getinfo(2, "S").source:sub(2)
    return str:match("(.*/)")
end
package.cpath = package.cpath .. ";" .. script_path() .. "?.so"

function split (inputstr, sep)
    if sep == nil then
        sep = "%s"
    end
    local t={}
    for str in string.gmatch(inputstr, "([^"..sep.."]+)") do
        table.insert(t, str)
    end
    return t
end
local version_list = split(_VERSION:sub(5), ".");
local interpreter_version = version_list[1] .. version_list[2]

local dt_base
if interpreter_version == "51" then
    dt_base = require("lua" .. interpreter_version .. "-dt_core_lua")
else
    dt_base = require("dt_core_lua-lua" .. interpreter_version)
end
local socket = require("socket")
local cjson = require("cjson")
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

local function startWith(str, substr)
    if str == nil or substr == nil then
        return nil, "the string or the sub-stirng parameter is nil"
    end
    if string.find(str, substr) ~= 1 then
        return false
    else
        return true
    end
end

local function fileExists(path)
    local retTable = { os.execute("cd " .. path) }
    local code = retTable[3] or retTable[1]
    return code == 0
end

local function isWindows()
    local separator = package.config:sub(1, 1)
    local osName = os.getenv("OS")
    local result = (separator == '\\' or (osName ~= nil and startWith(string.lower(osName), "windows")))
    return result
end

local function checkKV(properties, eventName)
    -- check K/V
    local userAdd = "#user_add"
    local userUnset = "#user_unset"
    for key, value in pairs(properties) do
        if (string.len(key) == 0) then
            DTLog.error("The property key is empty")
        end
        if (type(value) ~= "string" and
                type(value) ~= "number" and
                type(value) ~= "boolean" and
                type(value) ~= "table") then
            DTLog.error("The property " .. key .. " is not number, string, boolean, table.")
        end
        if (type(value) == "table") then
            for k, v in pairs(value) do
                if (type(v) ~= "string" and type(v) ~= "number" and type(v) ~= "boolean" and type(v) ~= "table") then
                    DTLog.error("The table property " .. k .. " is not number, string, boolean, table.")
                end
            end
        end
        if (type(value) == "string" and string.len(value) == 0 and not (userUnset == eventName)) then
            DTLog.error("The property " .. key .. " string value is null or empty")
        end

        if (userAdd == eventName and type(value) ~= "number") then
            DTLog.error("The property value of " .. key .. " should be a number ")
        end
    end
end

local function divide(properties)
    local presetProperties = {}
    local finalProperties = {}
    for key, value in pairs(properties) do
        if (key == "#android_id" or key == "#event_syn" or key == "#bundle_id" or key == "#event_time" or key == "#app_id" or key == "#gaid" or key == "#dt_id" or key == "#acid") then
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

local function check(dtId, acId, eventType, eventName, eventId, properties, dynamicSuperProperties, checkKeyAndValue)
    if checkKeyAndValue == nil or checkKeyAndValue == false then
        return
    end
    assert(dtId == nil or type(dtId) == "string" or type(dtId) == "number", "dtId must be string or number type")
    assert(acId == nil or type(acId) == "string" or type(acId) == "number", "acId must be string or number type")
    assert(type(eventType) == "string", "type must be string type")
    assert(eventName == nil or type(eventName) == "string", "eventName must be string type")
    assert(type(properties) == "table", "properties must be Table type")
    if dynamicSuperProperties ~= nil then
        assert(type(dynamicSuperProperties) == "table", "dynamicSuperProperties must be Table type")
        checkKV(dynamicSuperProperties, eventName)
    end
    -- check name
    if ((dtId == nil or string.len(dtId) == 0) and (acId == nil or string.len(acId) == 0)) then
        DTLog.error("dtId, acId can't both be empty")
    end
    if (Util.startWith(eventType, "track") and (eventName == nil or string.len(eventName) == 0)) then
        DTLog.error("eventName can't be empty when the type is track or track_update or track_overwrite")
    end
    if (Util.startWith(eventType, "track_")  and (eventId == nil or string.len(eventId) == 0)) then
        DTLog.error("eventId can't be empty when the type is track_update or track_overwrite")
    end
    checkKV(properties, eventName)
end

---
---@param consumer any
---@param dtId string
---@param acId string
---@param eventType string
---@param eventName string
---@param properties table
---@param superProperties table
---@param dynamicSuperPropertiesTracker function
---@param checkKeyAndValue boolean
local function upload(consumer, dtId, acId, eventType, eventName, properties, superProperties, dynamicSuperPropertiesTracker, checkKeyAndValue)
    local finalProperties, presetProperties = divide(properties)
    local dynamicSuperProperties = {}
    if dynamicSuperPropertiesTracker ~= nil and type(dynamicSuperPropertiesTracker) == "function" then
        dynamicSuperProperties = dynamicSuperPropertiesTracker()
        check(dtId, acId, eventType, eventName, eventId, finalProperties, dynamicSuperProperties, checkKeyAndValue)
    else
        check(dtId, acId, eventType, eventName, eventId, finalProperties, checkKeyAndValue)
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
    if dt_base.verify_event(eventJson) then
        ret = consumer:add(eventJson)
    end
    presetProperties = nil
    finalProperties = nil
    mergeProperties = nil
    eventJson = nil
    return ret
end

--- 
--- Init analytics instance
---@param self any
---@param consumer any logConsumer
---@param strictMode boolean enable properties check
DTAnalytics = class(function(self, consumer, strictMode)
    if consumer == nil or type(consumer) ~= "table" then
        DTLog.error("consumer params is invalidate.")
        return
    end
    self.consumer = consumer
    self.checkKeyAndValue = strictMode or DTAnalytics.strictMode
    self.superProperties = {}
    self.dynamicSuperPropertiesTracker = nil
    DTLog.info("SDK init success")
end)

--- Enable log or not
---@param enable boolean
function DTAnalytics.enableLog(enable)
    DTLog.enable = enable
end

--- Construct logConsumer
---@param self any
---@param logPath string
---@param rule string
---@param batchNum number
---@param fileSize number
---@param fileNamePrefix string
DTAnalytics.DTLogConsumer = class(function(self, logPath, rule, batchNum, fileSize, fileNamePrefix)
    if logPath == nil or type(logPath) ~= "string" or string.len(logPath) == 0 then
        DTLog.error("logPath can't be empty.")
    end
    if rule ~= nil and type(rule) ~= "string" then
        DTLog.error("rule is invalidate.")
    end

    if batchNum ~= nil and type(batchNum) ~= "number" then
        DTLog.error("batchNum is must be Number type.")
    end
    self.rule = rule or DTAnalytics.LOG_RULE.DAY
    self.logPath = Util.mkdirFolder(logPath)
    self.fileNamePrefix = fileNamePrefix
    self.fileSize = fileSize
    self.count = 0;
    self.file = nil;
    self.batchNum = batchNum or DTAnalytics.batchNumber
    self.currentFileTime = os.date("%Y-%m-%d %H")
    self.fileName = Util.getFileName(logPath, fileNamePrefix, self.rule)
    self.eventArrayJson = {}
    DTLog.info("Mode: log consumer. File path: " .. logPath)
end)

-- Retain file handler
DTAnalytics.DTLogConsumer.fileHandler = nil

function DTAnalytics.DTLogConsumer:add(msg)
    local num = #self.eventArrayJson + 1
    self.eventArrayJson[num] = msg

    DTLog.info("Enqueue data to buffer")

    if (num >= self.batchNum) then
        self:flush()
    end
    return num
end

function DTAnalytics.DTLogConsumer:flush()
    if #self.eventArrayJson == 0 then
        return true
    end
    local isFileNameChange = false
    if self.rule == DTAnalytics.LOG_RULE.HOUR then
        isFileNameChange = Util.getDateFromDateTime(self.currentFileTime) ~= os.date("%Y-%m-%d")
                or Util.getHourFromDate(self.currentFileTime) ~= Util.getCurrentHour()
    else
        isFileNameChange = Util.getDateFromDateTime(self.currentFileTime) ~= os.date("%Y-%m-%d")
    end

    if isFileNameChange or self.fileHandler == nil then
        self.currentFileTime = os.date("%Y-%m-%d %H:%M:%S")
        self.fileName = Util.getFileName(self.logPath, self.fileNamePrefix, self.rule)
        self.count = 0
        -- close old file handler and create new file handler
        if self.fileHandler then
            self.fileHandler:close()
        end
        local logFileName = self.fileName .. "_" .. self.count
        self.fileHandler = assert(io.open(logFileName, "a"))
    else
        if self.fileSize > 0 then
            self.count, self.fileHandler = Util.getFileHandlerAndCount(self.fileHandler, self.fileName, self.fileSize, self.count)
        end
    end

    local data = ""
    for key, value in pairs(self.eventArrayJson) do
        local json = Util.toJson(value)
        data = data .. json .. "\n"
    end

    DTLog.info("Flush data, count: [" .. #self.eventArrayJson .. "]\n" .. data)

    local result = self.fileHandler:write(data)
    if (result) then
        self.eventArrayJson = {}
    else
        DTLog.error("data write failed. count: ", #self.eventArrayJson)
    end

    self.fileHandler:flush()
    self.fileHandler:seek("end", 0)      

    return true
end

function DTAnalytics.DTLogConsumer:close()
    self:flush()
    -- close old file handler
    if self.fileHandler then
        self.fileHandler:close()
    end
    DTLog.info("Close log consumer")
end

--- Set dynamic common properties
---@param callback function
function DTAnalytics:setDynamicSuperProperties(callback)
    if callback ~= nil then
        self.dynamicSuperPropertiesTracker = callback
    end
end

--- Set common properties
---@param params table
function DTAnalytics:setSuperProperties(params)
    if self.checkKeyAndValue == true then
        local ok, ret = pcall(checkKV, params)
        if not ok then
            DTLog.error("common properties error: ", ret)
            return
        end
    end

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
    local ok, ret = pcall(upload, self.consumer, dtId, acId, "#user_set", nil, properties, self.checkKeyAndValue)
    if ok then
        return ret
    end
end

--- Set user properties, if such property had been set before, this message would be neglected.
---@param acId string
---@param dtId string
---@param properties table
function DTAnalytics:userSetOnce(acId, dtId, properties)
    local ok, ret = pcall(upload, self.consumer, dtId, acId, "#user_set_once", nil, properties, self.checkKeyAndValue)
    if ok then
        return ret
    end
end

--- To accumulate operations against the property
---@param acId string
---@param dtId string
---@param properties table
function DTAnalytics:userAdd(acId, dtId, properties)
    local ok, ret = pcall(upload, self.consumer, dtId, acId, "#user_add", nil, properties, self.checkKeyAndValue)
    if ok then
        return ret
    end
end

--- To add user properties of array type
---@param acId string
---@param dtId string
---@param properties table
function DTAnalytics:userAppend(acId, dtId, properties)
    local ok, ret = pcall(upload, self.consumer, dtId, acId, "#user_append", nil, properties, self.checkKeyAndValue)
    if ok then
        return ret
    end
end

--- Append user properties to array type by unique.
---@param acId string
---@param dtId string
---@param properties table
function DTAnalytics:userUniqAppend(acId, dtId, properties)
    local ok, ret = pcall(upload, self.consumer, dtId, acId, "#user_uniq_append", nil, properties, self.checkKeyAndValue)
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
    local ok, ret = pcall(upload, self.consumer, dtId, acId, "#user_unset", nil, unSetProperties, self.checkKeyAndValue)
    if ok then
        return ret
    end
end

--- Delete a user, This operation cannot be undone
---@param acId string
---@param dtId string
function DTAnalytics:userDelete(acId, dtId, properties)
    local ok, ret = pcall(upload, self.consumer, dtId, acId, "#user_delete", nil, properties, self.checkKeyAndValue)
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
    local ok, ret = pcall(upload, self.consumer, dtId, acId, "track", eventName, properties, self.superProperties, self.dynamicSuperPropertiesTracker, self.checkKeyAndValue)
    if ok then
        return ret
    end
end

--- Flush data
function DTAnalytics:flush()
    self.consumer:flush()
end

--- Close SDK
function DTAnalytics:close()
    self.consumer:close()
    DTLog.info("SDK closed.")
end

function DTAnalytics:toString()
    return self.consumer:toString()
end

DTAnalytics.platForm = "dt_lua_sdk"
DTAnalytics.version = "1.0.0"
DTAnalytics.batchNumber = 20
DTAnalytics.strictMode = true
DTAnalytics.cacheCapacity = 50
DTAnalytics.logModePath = "."

--- Log file rotate type
DTAnalytics.LOG_RULE = {}
--- Log file rotate type: By hour
DTAnalytics.LOG_RULE.HOUR = "%Y-%m-%d-%H"
--- Log file rotate type: By Day
DTAnalytics.LOG_RULE.DAY = "%Y-%m-%d"

function Util.toJson(eventArrayJson)
    return cjson.encode(eventArrayJson)
end

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

function Util.mkdirFolder(path)
    if (fileExists(path)) then
        return path
    end
    local isWindows = isWindows()
    local cmd = "mkdir -p " .. path
    if (isWindows) then
        cmd = "mkdir " .. path
    end
    local retTable = { os.execute(cmd) }
    local code = retTable[3] or retTable[1]
    if (code ~= 0) then
        if (isWindows) then
            return os.getenv("TEMP")
        else
            return "/tmp"
        end
    end
    return path
end

function Util.writeFile(fileName, eventArrayJson)
    if #eventArrayJson == 0 then
        return false
    end
    if Util.fileHandler == nil then
        Util.fileHandler = assert(io.open(fileName, 'a'))
    end
    local file = Util.fileHandler
    -- local file = assert(io.open(fileName, 'a'))
    local data = ""
    for i = 1, #eventArrayJson do
        local json = Util.toJson(eventArrayJson[i])
        data = data .. json .. "\n"
    end
    file:write(data)
    -- file:close()
    -- file = nil
    return true
end

function Util.getFileName(filePath, fileNamePrefix, rule)
    local isWindows = isWindows()
    local separator = "/"
    if (isWindows) then
        separator = "\\"
    end
    local fileName
    if not fileNamePrefix or #fileNamePrefix == 0 then
        fileName = filePath .. separator .. "log." .. os.date(rule)
    else
        fileName = filePath .. separator .. fileNamePrefix .. ".log." .. os.date(rule)
    end

    return fileName
end

--- func desc
---@param currentFile file*
---@param fileName string
---@param fileSize number
---@param count number
---@return number file count
---@return file* effective handler
function Util.getFileHandlerAndCount(currentFile, fileName, fileSize, count)
    if not count then
        count = 0
    end

    local finalFileName = nil
    local file = currentFile

    while file
    do
        local len = assert(file:seek("end"))
        if len < (fileSize * 1024 * 1024) then
            -- get effective file handler
            break
        else
            count = count + 1
            finalFileName = fileName .. "_" .. count
            -- close old file
            file:close()
            -- create new file
            file = assert(io.open(finalFileName, "a"))
        end
    end
    return count, file
end

function Util.startWith(str, substr)
    if str == nil or substr == nil then
        return false
    end
    if string.find(str, substr) ~= 1 then
        return false
    else
        return true
    end
end

function Util.tablecopy(src, dest)
    for k, v in pairs(src) do
        if type(v) == "table" then
            dest[k] = {}
            Util.tablecopy(v, dest[k])
        else
            dest[k] =  v
        end
    end
end

function Util.create_uuid()
    local uuidLib = require("uuid")
    return uuidLib()
end

function Util.getHourFromDate(dateString)
    return string.sub(dateString, 12, 13)
end

function Util.getDateFromDateTime(dateTime)
    return string.sub(dateTime, 1, 10)
end
function Util.getCurrentHour()
    local t = os.date("%Y-%m-%d %H:%M:%S")
    if type(t) == "string" then
        return string.sub(t, 12, 13)
    end
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
