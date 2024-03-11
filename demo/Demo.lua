package.path = package.path .. ";../?.lua"
local dtAnalytics = require("src.DataTowerSdk")

local function getLogConsumer()
	return dtAnalytics.DTLogConsumer("./log", dtAnalytics.LOG_RULE.HOUR, 200, 500)
end

dtAnalytics.enableLog(true)

local consumer = getLogConsumer()

--- init SDK with consumer
local sdk = dtAnalytics(consumer, false)

local dtId = "1234567890987654321"
local acId = nil

-- set dynamic super properties
sdk:setDynamicSuperProperties(function ()
    local properties = {}
	properties["DynamicKey"] = "DynamicValue"
	return properties
end)

-- set super properties
local superProperties = {}
superProperties["super_key_sex"] = "male"
superProperties["super_key_age"] = 23
sdk:setSuperProperties(superProperties)
superProperties = nil

local properties = {}
properties["productNames"] = { "Lua", "hello" }
properties["productType"] = "Lua book"
properties["producePrice"] = 80
properties["shop"] = "xx-shop"
properties["#os"] = "1.1.1.1"
properties["date"] = os.date()
properties["date1"] = os.date("%Y-%m-%d %H:%M:%S")
properties["sex"] = 'female';

sdk:track(acId, dtId, "eventName", properties)

sdk:clearSuperProperties()

sdk:track(acId, dtId, "eventName", properties)

 properties = {}
 properties["productNames"] = { "Lua", "hello" }
 properties["productType"] = "Lua book"
 properties["producePrice"] = 80
 properties["shop"] = "xx-shop"
 properties["#os"] = "1.1.1.1"
 properties["date"] = os.date()
 properties["date1"] = os.date("%Y-%m-%d %H:%M:%S")
 properties["sex"] = 'female';
 sdk:track(acId, dtId, "current_online", properties)

 local profiles = {}
 profiles["#city"] = "beijing"
 profiles["#province"] = "beijing"
 profiles["nickName"] = "nick name 123"
 profiles["userLevel"] = 0
 profiles["userPoint"] = 0
 profiles["#os"] = "1.2.3"
 local interestList = { "sport", "football", "game" }
 profiles["interest"] = interestList
 sdk:userSet(acId, dtId, profiles)
 profiles = nil

 local profiles = {}
 profiles["setOnceKey"] = "setOnceValue"
 sdk:userSetOnce(acId, dtId, profiles)

 profiles["setOnceKey"] = "setTwice"
 sdk:userSetOnce(acId, dtId, profiles)

 profiles = {}
 profiles["userPoint"] = 100
 sdk:userAdd(acId, dtId, profiles)

 profiles = {}
 profiles["append"] = { "test_append" }
 sdk:userAppend(acId, dtId, profiles)

 profiles = {}
 profiles["append"] = {"test_append", "test_append1"}
 sdk:userUniqAppend(acId, dtId, profiles)

sdk:flush()
sdk:close()