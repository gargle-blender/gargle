
-- binName
local data = 'data'

local preTime = 'preTime'
local capacity = 1000

-- 令牌桶参数
local bucket = 'bucket'
local putRateKey = 'putRate'
local capacityKey = 'capacity'
local putRate = 1


--漏水桶参数
local water = 'water'

local function getBinMap(recode, binName)
    local binMap = recode[binName]
    if binMap == nil then
        binMap = map()
    end

    return binMap
end

local function TokenBucketLimiter(recode, params)
    local response = map()
    local putRateValue = putRate
    local capacityValue = capacity
    if params[putRateKey] ~= nil then
        putRateValue = params[putRateKey]
    end

    if params[capacityKey] ~= nil then
        capacityValue = params[capacityKey]
    end

    local nowTimeValue = os.time(date)
    local paas = false
    -- 第一次请求初始化. 注意: 直接用 recode[data] 操作数据是不会更新的!!!
    local dataMap = getBinMap(recode, data)
    if dataMap == nil or map.size(dataMap) == 0 then
        dataMap = map()
        dataMap[preTime] = nowTimeValue
        dataMap[bucket] = capacityValue - 1
        paas = true
    else
        local preTimeValue =  dataMap[preTime]
        local bucketValue = math.min(capacityValue,  dataMap[bucket] + (nowTimeValue - preTimeValue) * putRateValue)
        dataMap[preTime] = nowTimeValue
        if bucketValue <= 0 then
            bucketValue = 0
        else
            paas = true
            bucketValue = bucketValue - 1
        end
        dataMap[bucket] = bucketValue
    end
    response['pass'] = paas
    response['data'] = dataMap
    response['now'] = nowTimeValue
    -- 整个bin设置回来, 注意: 直接用 recode[data] 操作数据是不会更新的!!!
    recode[data] = dataMap
    aerospike:update(recode)
    return response
end


local function LeakyBucketLimiter(recode, params)
    return 'true'
end


local Functions = {
    LeakyBucketLimiter = LeakyBucketLimiter,
    TokenBucketLimiter = TokenBucketLimiter,
}

function entrance(recode, params)
    local response = map()
    response[RESPONSE_CODE_KEY] = RESPONSE_PARAM_ERROR_CODE
    response[RESPONSE_MSG_KEY] = RESPONSE_PARAM_ERROR_MSG
    if (not aerospike:exists(recode)) then
        local rc = aerospike:create(recode)
        if (rc == nil) then
            response[RESPONSE_CODE_KEY] = RESPONSE_CREATE_ERROR_CODE
            response[RESPONSE_MSG_KEY] = RESPONSE_CREATE_ERROR_MSG
            return response;
        end
    end

    if params == nil or map.size(params) == 0 then
        return response
    end

    local resultMap = map()
    for k,v in map.pairs(params) do
        local fun = Functions[k]
        if fun == nil then
            response[RESPONSE_MSG_KEY] = 'function does not exist'
            response['params'] = params
            return response;
        end
    end

    for k,v in map.pairs(params) do
        local fun = Functions[k]
        resultMap[k] = fun(recode, v)
    end

    return resultMap
end