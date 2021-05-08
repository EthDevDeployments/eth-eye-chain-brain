import Web3 from "web3"
import uniswapPair from './abis/uniswapPair.json'
import { processRecords } from "./aggregation"
import { RecordEntity } from "./types"
import addresses from './addresses.json'
import { rationalize } from "./helpers"
const abiDecoder = require('abi-decoder')



// construct web3 and decoder objects
const web3 = new Web3(new Web3.providers.WebsocketProvider('ws://3.239.13.11:8546'))
abiDecoder.addABI(uniswapPair)

async function main() {

    var globalRecs: any = {}

    web3.eth.subscribe('logs', {}).on('data', async log => {
        const poolAddress: string = log.address.toLowerCase()

        if (!Object.keys(addresses.pairs).includes(poolAddress)) {
            return
        }

        const namePair = addresses.pairs[poolAddress].split(',', 2)

        try {
            // decode logs into serialized format
            let decodedLogs = abiDecoder.decodeLogs([log]);

            // iterate over logs (should only be one)
            for (const decodedLog of decodedLogs) {
                if (decodedLog.name == 'Sync') {
                    console.log('\ngot a hit')
                    const reserve1: bigint = BigInt(decodedLog.events[0].value)
                    const reserve2: bigint = BigInt(decodedLog.events[1].value)

                    globalRecs[log.transactionHash + poolAddress] = {
                        poolAddress,
                        blockNumber: log.blockNumber,
                        timestamp: Math.floor(Date.now() / 1000),
                        token1: {
                            tokenName: namePair[0],
                            tokenAddress: addresses[namePair[0]]
                        },
                        token2: {
                            tokenName: namePair[1],
                            tokenAddress: addresses[namePair[1]]
                        },
                        reserve1,
                        reserve2,
                        reserveRatio: rationalize(reserve1, reserve2),
                        transactionVolume: {}
                    }
                }

                if (decodedLog.name == 'Swap') {
                    var rec = globalRecs[log.transactionHash + poolAddress]

                    if (rec == undefined) return

                    for (const event of decodedLog.events) {
                        if (event.name == 'amount0In') {
                            rec.transactionVolume.token1In = BigInt(event.value)
                        } else if (event.name == 'amount0Out') {
                            rec.transactionVolume.token1Out = BigInt(event.value)
                        } else if (event.name == 'amount1In') {
                            rec.transactionVolume.token2In = BigInt(event.value)
                        } else if (event.name == 'amount1Out') {
                            rec.transactionVolume.token2Out = BigInt(event.value)
                        }
                    }

                    delete globalRecs[log.transactionHash + poolAddress]
                    console.log(rec)
                    processRecords([rec] as RecordEntity[], true)
                }
            }
        } catch (err) {
            console.log(err)
        }
    })
}

main()