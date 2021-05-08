import { DynamoDB, SharedIniFileCredentials, config } from "aws-sdk"
import { AggregationEntity, BlockIntervalQuery, DdbAggregationEntity, DdbRecordEntity, RecordEntity, RecordQuery, TimeIntervalQuery } from "./types"
import { Granularity } from "aws-sdk/clients/costexplorer"


// the following line is just for local config
config.credentials = new SharedIniFileCredentials({ profile: 'eth-dev' })


// config
const ddbClient = () => new DynamoDB.DocumentClient({ region: 'us-east-1' })




/*
 *  GENERIC HELPERS
 */

function handleError(err: any, reject: (reason?: any) => void) {
    console.log(err)
    reject(err)
}


function bigIntToString(obj: object, keys: string[]) {
    for (const outKey of keys) {
        if (typeof obj[outKey] == 'string') {
            continue
        }

        if (typeof obj[outKey] == 'bigint') {
            obj[outKey] = obj[outKey].toString()
            continue
        }

        for (const key of Object.keys(obj[outKey])) {
            obj[outKey][key] = obj[outKey][key].toString()
        }
    }
}


export function rationalize(reserve1: bigint, reserve2: bigint) {
    return Number(reserve1 * BigInt('1000000000000') / reserve2) / 1000000000000
}




/*
 *  DYNAMODB HELPERS
 */

function putItem<T>(tableName: string, item: T) {
    return new Promise((resolve, reject) => {
        ddbClient().put(
            {
                TableName: tableName,
                Item: item
            },
            (err, _data) => {
                if (err) {
                    return handleError(err, reject)
                }

                resolve(true)
            }
        )
    })
}


export function getItem<T>(tableName: string, key: object) {
    return new Promise((resolve, reject) => {
        ddbClient().get({
            TableName: tableName,
            Key: key
        }, (err, data) => {
            if (err) {
                return handleError(err, reject)
            }

            resolve(data.Item as T)
        })
    })
}




/*
 *  AGGREGATION HELPERS
 */

function aggregationToDdbAggregation(aggregation: AggregationEntity) {
    var aggregationToEnter: any = {
        poolAddressGranularity: toAggregationHashKey(aggregation.poolAddress, aggregation.granularity),
        ...aggregation
    }

    delete aggregationToEnter.poolAddress
    delete aggregationToEnter.granularity

    bigIntToString(aggregationToEnter, ['reserve1', 'reserve2', 'transactionVolume'])
    return aggregationToEnter
}


export function putAggregation(aggregation: AggregationEntity) {
    return putItem<DdbAggregationEntity>('AggregationsTable', aggregationToDdbAggregation(aggregation))
}


export function putDdbAggregation(aggregation: DdbAggregationEntity) {
    return putItem<DdbAggregationEntity>('AggregationsTable', aggregation)
}


function fromAggregationHashKey(str: string) {
    const [poolAddress, granularity] = str.split(',', 2)
    return { poolAddress, granularity }
}


export function toAggregationHashKey(poolAddress: string, granularity: string) {
    return poolAddress + ',' + granularity
}


function deserializeDdbAggregationHashKey(aggregation: DdbAggregationEntity) {
    var aggregationEntity: any = aggregation
    const { poolAddress, granularity } = fromAggregationHashKey(aggregationEntity.poolAddressGranularity)
    delete aggregationEntity.poolAddressGranularity
    return {
        poolAddress,
        granularity,
        ...aggregationEntity
    }
}


export function newAggregationCycleObject(rec: RecordEntity, granularity: Granularity, startTimestamp: number, endTimestamp: number): DdbAggregationEntity {
    const record: DdbRecordEntity = recordToDdbRecord(rec)

    return {
        poolAddressGranularity: toAggregationHashKey(record.poolAddress, granularity),
        startBlockNumber: record.blockNumber,
        endBlockNumber: record.blockNumber,
        startTimestamp,
        endTimestamp,
        token1: record.token1,
        token2: record.token2,
        reserve1: {
            first: record.reserve1,
            min: record.reserve1,
            max: record.reserve1,
            last: record.reserve1
        },
        reserve2: {
            first: record.reserve2,
            min: record.reserve2,
            max: record.reserve2,
            last: record.reserve2
        },
        reserveRatio: {
            first: record.reserveRatio,
            min: record.reserveRatio,
            max: record.reserveRatio,
            last: record.reserveRatio
        },
        transactionVolume: {
            token1In: {
                total: record.transactionVolume.token1In,
                first: record.transactionVolume.token1In,
                min: record.transactionVolume.token1In,
                max: record.transactionVolume.token1In,
                last: record.transactionVolume.token1In
            },
            token1Out: {
                total: record.transactionVolume.token1Out,
                first: record.transactionVolume.token1Out,
                min: record.transactionVolume.token1Out,
                max: record.transactionVolume.token1Out,
                last: record.transactionVolume.token1Out
            },
            token2In: {
                total: record.transactionVolume.token2In,
                first: record.transactionVolume.token2In,
                min: record.transactionVolume.token2In,
                max: record.transactionVolume.token2In,
                last: record.transactionVolume.token2In
            },
            token2Out: {
                total: record.transactionVolume.token2Out,
                first: record.transactionVolume.token2Out,
                min: record.transactionVolume.token2Out,
                max: record.transactionVolume.token2Out,
                last: record.transactionVolume.token2Out
            }
        }
    }
}




/*
 *  RECORD HELPERS
 */
function recordToDdbRecord(record: RecordEntity) {
    var res: any = record
    bigIntToString(res, ['reserve1', 'reserve2', 'transactionVolume'])
    return res
}


export function putRecord(record: RecordEntity) {
    return putItem<DdbRecordEntity>('RecordsTable', recordToDdbRecord(record))
}




/*
 *  QUERY HELPERS
 */

function isBlockIntervalQuery(query: RecordQuery): query is BlockIntervalQuery {
    return Object.keys(query).includes('startBlock')
}


function isTimeIntervalQuery(query: RecordQuery): query is TimeIntervalQuery {
    return Object.keys(query).includes('startTime')
}


// TODO: deal with 1MB query limit using loop
function defaultQueryRecords(query: RecordQuery) {
    if (isTimeIntervalQuery(query)) {
        return new Promise((resolve, reject) => {
            ddbClient().query({
                TableName: 'RecordsTable',
                IndexName: 'PoolAddress_Timestamp_GSI',
                KeyConditionExpression: 'poolAddress = :poolAddress AND timestamp BETWEEN :startTime AND :endTime',
                ExpressionAttributeValues: {
                    ':poolAddress': query.poolAddress,
                    ':startTime': query.startTime,
                    ':endTime': query.endTime ?? Math.floor(Date.now() / 1000)
                },
                ScanIndexForward: true     // ascending order
            }, (err, data) => {
                if (err) {
                    return handleError(err, reject)
                }

                resolve(data.Items as DdbRecordEntity[])
            })
        })
    }

    else if (isBlockIntervalQuery(query)) {
        return new Promise((resolve, reject) => {
            ddbClient().query({
                TableName: 'RecordsTable',
                KeyConditionExpression: 'poolAddress = :poolAddress AND blockNumber BETWEEN :startBlock AND :endBlock',
                ExpressionAttributeValues: {
                    ':poolAddress': query.poolAddress,
                    ':startBlock': query.startBlock,
                    ':endBlock': query.endBlock
                },
                ScanIndexForward: true     // ascending order
            }, (err, data) => {
                if (err) {
                    return handleError(err, reject)
                }

                resolve(data.Items as DdbRecordEntity[])
            })
        })
    }

    return
}


// TODO: fix this shit
export function reduceRecords(records: RecordEntity[], skipDuplicates: boolean): RecordEntity[] {
    var seenPools = new Set<string>()
    var res: RecordEntity[] = []

    for (const record of records) {
        if (seenPools.has(record.poolAddress) && skipDuplicates) {
            continue
        }

        else if (seenPools.has(record.poolAddress)) {
            const idx: number = res.findIndex(rec => rec.poolAddress == record.poolAddress)
            res[idx].reserve1.amount += record.reserve1.amount
            res[idx].reserve2.amount += record.reserve2.amount
        }

        else {
            seenPools.add(record.poolAddress)
            res.push(record)
        }
    }

    return res
}


export function queryRecords(query: RecordQuery, granularity?: Granularity) {
    if (!granularity) {
        return defaultQueryRecords(query)
    }

    const poolAddressGranularity: string = toAggregationHashKey(query.poolAddress, granularity)

    if (isTimeIntervalQuery(query)) {
        return new Promise((resolve, reject) => {
            ddbClient().query({
                TableName: 'AggregationsTable',
                KeyConditionExpression: 'poolAddressGranularity = :poolAddressGranularity AND startTimestamp BETWEEN :startTime AND :endTime',
                ExpressionAttributeValues: {
                    ':poolAddressGranularity': poolAddressGranularity,
                    ':startTime': query.startTime,
                    ':endTime': query.endTime ?? Math.floor(Date.now() / 1000)
                },
                ScanIndexForward: true     // ascending order
            }, (err, data) => {
                if (err) {
                    return handleError(err, reject)
                }

                resolve(data.Items?.map(entry => deserializeDdbAggregationHashKey(entry as DdbAggregationEntity)) ?? [])
            })
        })
    }

    else if (isBlockIntervalQuery(query)) {
        return new Promise((resolve, reject) => {
            ddbClient().query({
                TableName: 'AggregationsTable',
                IndexName: 'PoolAddressGranularity_StartBlockNumber_GSI',
                KeyConditionExpression: 'poolAddressGranularity = :poolAddressGranularity AND startBlockNumber BETWEEN :startBlock AND :endBlock',
                ExpressionAttributeValues: {
                    ':poolAddressGranularity': poolAddressGranularity,
                    ':startBlock': query.startBlock,
                    ':endBlock': query.endBlock
                },
                ScanIndexForward: true     // ascending order
            }, (err, data) => {
                if (err) {
                    return handleError(err, reject)
                }

                resolve(data.Items?.map(entry => deserializeDdbAggregationHashKey(entry as DdbAggregationEntity)) ?? [])
            })
        })
    }

    return
}
