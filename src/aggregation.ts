import {
    newAggregationCycleObject, putRecord, reduceRecords, putDdbAggregation, rationalize
} from "./helpers";
import { granularityToSec, RecordEntity, Granularity } from "./types";
import fs from 'fs'



//  Handle insertion and aggregation for a single block
export function processRecords(records: RecordEntity[], skipDuplicates: boolean) {
    // const recs: RecordEntity[] = reduceRecords(records, skipDuplicates)

    for (const record of records) {
        // add record to table
        console.log('putting record...')
        putRecord(record)

        // initialize/aggregate/flush as needed
        const headerPath: string = 'local-aggregation/header.json'
        console.log('starting aggregation...')
        updateAndFlushAggregationObjects(record, headerPath)
        console.log('aggregation succeeded\n')
    }
}


function updateAndFlushAggregationObjects(record: RecordEntity, headerPath: string) {
    var header: any = JSON.parse(fs.readFileSync(headerPath, 'utf-8'))

    // one-time initialization of global time benchmark
    if (header.globalStart == 0) {
        header.globalStart = record.timestamp
        fs.writeFileSync(headerPath, JSON.stringify(header))
    }

    const poolPath: string = 'local-aggregation/' + record.poolAddress + '.json'

    // if no aggregations have been made at all for this pool
    if (!fs.existsSync(poolPath)) {
        var pool: any = {}

        for (const granularity of Object.keys(granularityToSec)) {

            // initialize and populate new granularity sub-object
            initializeAggregation(record, granularity as Granularity, header, pool)
        }

        fs.writeFileSync(poolPath, JSON.stringify(pool))
    }

    // if some aggregations have been made for this pool
    else {
        var pool: any = JSON.parse(fs.readFileSync(poolPath, 'utf-8'))

        for (const granularity of Object.keys(granularityToSec)) {

            // if this granularity has not yet been aggregated
            if (pool[granularity] == undefined) {
                initializeAggregation(record, granularity as Granularity, header, pool)
            }

            // if it's time to flush this aggregation object
            else if (record.timestamp >= pool[granularity]['flushAt']) {
                putDdbAggregation(pool[granularity]['aggregation'])
                initializeAggregation(record, granularity as Granularity, header, pool)
            }

            // actual aggregation logic
            else {

                // update last block number
                pool[granularity].aggregation.endBlockNumber = record.blockNumber

                // update reserves and reserve ratio
                updateReserve(record, granularity as Granularity, pool, 'reserve1')
                updateReserve(record, granularity as Granularity, pool, 'reserve2')
                updateReserveRatio(record, granularity as Granularity, pool)

                // update transactionVolume
                for (const tkn of Object.keys(pool[granularity].aggregation.transactionVolume)) {
                    updateTransactionVolume(record, granularity as Granularity, pool, tkn)
                }

                putDdbAggregation(pool[granularity]['aggregation'])
            }
        }

        fs.writeFileSync(poolPath, JSON.stringify(pool))
    }
}


function initializeAggregation(record: RecordEntity, granularity: Granularity, header: any, pool: any) {

    // flush at first multiple of granularity since globalStart following this event 
    const startTimestamp: number = header.globalStart + granularityToSec[granularity] * Math.floor((record.timestamp - header.globalStart) / granularityToSec[granularity])
    const endTimestamp: number = startTimestamp + granularityToSec[granularity]
    pool[granularity] = {}
    pool[granularity]['flushAt'] = endTimestamp
    pool[granularity]['aggregation'] = newAggregationCycleObject(record, granularity, startTimestamp, endTimestamp)
    putDdbAggregation(pool[granularity]['aggregation'])
}


function updateReserve(record: RecordEntity, granularity: Granularity, pool: any, reserve: 'reserve1' | 'reserve2') {
    pool[granularity].aggregation[reserve].last = record[reserve].toString()
    pool[granularity].aggregation[reserve].min =
        BigInt(pool[granularity].aggregation[reserve].min) < BigInt(record[reserve]) ?
            pool[granularity].aggregation[reserve].min : record[reserve].toString()
    pool[granularity].aggregation[reserve].max =
        BigInt(pool[granularity].aggregation[reserve].max) > BigInt(record[reserve]) ?
            pool[granularity].aggregation[reserve].max : record[reserve].toString()
}


function updateReserveRatio(record: RecordEntity, granularity: Granularity, pool: any) {
    pool[granularity].aggregation.reserveRatio.last = record.reserveRatio
    pool[granularity].aggregation.reserveRatio.min =
        pool[granularity].aggregation.reserveRatio.min < record.reserveRatio ?
            pool[granularity].aggregation.reserveRatio.min : record.reserveRatio
    pool[granularity].aggregation.reserveRatio.max =
        pool[granularity].aggregation.reserveRatio.max > record.reserveRatio ?
            pool[granularity].aggregation.reserveRatio.max : record.reserveRatio
}


function updateTransactionVolume(record: RecordEntity, granularity: Granularity, pool: any, tkn: string) {
    pool[granularity].aggregation.transactionVolume[tkn].total =
        (BigInt(record.transactionVolume[tkn]) + BigInt(pool[granularity].aggregation.transactionVolume[tkn].total)).toString()
    pool[granularity].aggregation.transactionVolume[tkn].min =
        BigInt(record.transactionVolume[tkn]) < BigInt(pool[granularity].aggregation.transactionVolume[tkn].min) ?
            record.transactionVolume[tkn].toString() : pool[granularity].aggregation.transactionVolume[tkn].min
    pool[granularity].aggregation.transactionVolume[tkn].max =
        BigInt(record.transactionVolume[tkn]) > BigInt(pool[granularity].aggregation.transactionVolume[tkn].max) ?
            record.transactionVolume[tkn].toString() : pool[granularity].aggregation.transactionVolume[tkn].max
    pool[granularity].aggregation.transactionVolume[tkn].last = record.transactionVolume[tkn].toString()
}
