import * as fs from "fs/promises";
import axios from 'axios';
import { DuneClient, ContentType, ColumnType } from "@duneanalytics/client-sdk";

interface TokenInfo {
    decimals: string;
    name: string;
    symbol: string;
}

interface PoolData {
    liquidity: string;
    liquidityProviderCount: string;
    id: string;
    sqrtPrice: string;
    token0Price: string;
    token1Price: string;
    token0: TokenInfo;
    token1: TokenInfo;
}

function jsonToNDJSON(jsonArray: any[]): Buffer {
    return Buffer.from(jsonArray.map(item => JSON.stringify(item)).join('\n'));
}


async function fetchDataFromGraph() {
    const endpoint = 'https://api.thegraph.com/subgraphs/name/beamswap/beamswap-v3';
    const query = `
    query MyQuery {
        pools(orderBy: volumeUSD) {
            liquidity
            liquidityProviderCount
            id
            sqrtPrice
            token0Price
            token1Price
            token0 {
                decimals
                name
                symbol
            }
            token1 {
                decimals
                name
                symbol
            }
        }
    }`;

    try {
        const response = await axios({
            url: endpoint,
            method: 'post',
            data: {
                query
            }
        });

        // Flattening the data structure to match the defined schema
        return response.data.data.pools.map((pool: PoolData) => ({
            liquidity: parseFloat(pool.liquidity),
            liquidity_provider_count: parseInt(pool.liquidityProviderCount),
            id: pool.id,
            sqrt_price: parseFloat(pool.sqrtPrice),
            token0_price: parseFloat(pool.token0Price),
            token1_price: parseFloat(pool.token1Price),
            token0_decimals: parseInt(pool.token0.decimals),
            token0_name: pool.token0.name,
            token0_symbol: pool.token0.symbol,
            token1_decimals: parseInt(pool.token1.decimals),
            token1_name: pool.token1.name,
            token1_symbol: pool.token1.symbol
        }));
    } catch (error) {
        console.error("Error fetching data from The Graph:", error);
        return null;
    }
}


async function main() {
    const client = new DuneClient(process.env.DUNE_API_KEY!);

    const graphData = await fetchDataFromGraph();
    console.log(graphData);

    const namespace = "substrate";
    const table_name = "beamswap_pools_v3";
    try{
        const result = await client.table.delete({
          namespace: namespace,
          table_name: table_name
        });

    } catch (error) {}

    const result1 = await client.table.create({
      namespace: namespace,
      table_name: table_name,
      schema: [
             {"name": "liquidity", "type": ColumnType.Double},
            {"name": "liquidity_provider_count", "type": ColumnType.Integer},
            {"name": "id", "type": ColumnType.Varchar},
            {"name": "sqrt_price", "type": ColumnType.Double},
            {"name": "token0_price", "type": ColumnType.Double},
            {"name": "token1_price", "type": ColumnType.Double},
            {"name": "token0_decimals", "type": ColumnType.Integer},
            {"name": "token0_name", "type": ColumnType.Varchar},
            {"name": "token0_symbol", "type": ColumnType.Varchar},
            {"name": "token1_decimals", "type": ColumnType.Integer},
            {"name": "token1_name", "type": ColumnType.Varchar},
            {"name": "token1_symbol", "type": ColumnType.Varchar}   ]
    });
    console.log(result1);
    const result = await client.table.insert({
        namespace: namespace,
        table_name: table_name,
        data: jsonToNDJSON(graphData), // Directly use the JSON data
        content_type: ContentType.NDJson
    });
    console.log(result);
}

// Call the main function
main().catch(console.error);
