// type of token (currency)
type Token = {
    tokenAddress: string
    tokenName: string
}



// type of data-visualization candlestick + ddb equivalent
type Candlestick = {
    first: bigint
    min: bigint
    max: bigint
    last: bigint
}

type NumberCandlestick = {
    first: number
    min: number
    max: number
    last: Number
}

type DdbCandlestick = {
    first: string
    min: string
    max: string
    last: string
}



// type of transaction volume (record-level) + ddb equivalent
type Volume = {
    token1In: bigint
    token1Out: bigint
    token2In: bigint
    token2Out: bigint
}

type DdbVolume = {
    token1In: string
    token1Out: string
    token2In: string
    token2Out: string
}



// type of total + ddb equivalent
type Total = {
    total: bigint
}

type DdbTotal = {
    total: string
}



// type of transaction volume (aggregation-level) + ddb equivalent
type AggregationVolume = {
    token1In: Total & Candlestick
    token1Out: Total & Candlestick
    token2In: Total & Candlestick
    token2Out: Total & Candlestick
}

type DdbAggregationVolume = {
    token1In: DdbTotal & DdbCandlestick
    token1Out: DdbTotal & DdbCandlestick
    token2In: DdbTotal & DdbCandlestick
    token2Out: DdbTotal & DdbCandlestick
}



// type of record + ddb equivalent
export type RecordEntity = {
    poolAddress: string
    blockNumber: number
    token1: Token
    token2: Token
    timestamp: number // epoch sec
    reserve1: bigint
    reserve2: bigint
    reserveRatio: number
    transactionVolume: Volume
}

export type DdbRecordEntity = {
    poolAddress: string
    blockNumber: number
    token1: Token
    token2: Token
    timestamp: number // epoch sec
    reserve1: string
    reserve2: string
    reserveRatio: number
    transactionVolume: DdbVolume
}



// time granularity and type equivalent
export const granularityToSec = {
    "1MIN": 60,
    "5MIN": 300,
    "15MIN": 900,
    "30MIN": 1800,
    "1HR": 3600,
    "3HR": 10800,
    "6HR": 21600,
    "12HR": 43200,
    "1D": 86400,
    "3D": 259200,
    "1WK": 604800,
    "1MO": 2629800,
    "3MO": 7889400,
    "6MO": 15778800,
    "1YR": 31557600
}
export type Granularity = keyof typeof granularityToSec



// type of aggregation + ddb equivalent
export type AggregationEntity = {
    poolAddress: string
    granularity: Granularity
    startBlockNumber: number
    endBlockNumber: number
    startTimestamp: number
    endTimestamp: number
    token1: Token
    token2: Token
    reserve1: Candlestick
    reserve2: Candlestick
    reserveRatio: NumberCandlestick
    transactionVolume: AggregationVolume
}

export type DdbAggregationEntity = {
    poolAddressGranularity: string
    startBlockNumber: number
    endBlockNumber: number
    startTimestamp: number
    endTimestamp: number
    token1: Token
    token2: Token
    reserve1: DdbCandlestick
    reserve2: DdbCandlestick
    reserveRatio: NumberCandlestick
    transactionVolume: DdbAggregationVolume
}



// type of query for hash key in RecordsTable
type RecordHashQuery = {
    poolAddress: string
}



// type of query for records/aggregations within a time interval
export type TimeIntervalQuery = RecordHashQuery & {
    startTime: number
    endTime?: number
    startBlock?: never
    endBlock?: never
}



// type of query for records/aggregations within a block interval
export type BlockIntervalQuery = RecordHashQuery & {
    startBlock: number
    endBlock: number
    startTime?: never
    endTime?: never
}



// type of query to Records/AggregationsTable
export type RecordQuery = TimeIntervalQuery | BlockIntervalQuery
