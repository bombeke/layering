// @ts-nocheck
import { Client } from "@elastic/elasticsearch";
import { BulkResponse } from "@elastic/elasticsearch/lib/api/types";
import chunk from 'lodash/chunk';

export const client = new Client({ 
    node: "http://localhost:9200"
});

const processBulkInserts = (inserted: BulkResponse) => {
    const total = inserted.items.length;
    const errors = inserted.items.flatMap(({ index }) => {
        if (index?.error) return index.error?.caused_by;
        return [];
    });

    console.log(`Total:${total}`);
    console.log(`Errors:${errors.length}`);
    console.log(errors);
};

/**
 * Index a given array of documents in chunks of 250.
 *
 * @param index - The name of the index to use.
 * @param data - The array of documents to index.
 * @returns An array of promises that resolve when the respective chunk of documents has been indexed.
 */
export const indexBulk = async (index: string, data: any[]) => {
    const body = data.flatMap((doc) =>{
        return [
            { 
                index: { 
                    _index: index, 
                    //_id: doc["id"] 
                } 
            },
            doc,
        ];
    });
    const response = await client.bulk({
        refresh: true,
        body
    });
    console.log("Response:",response)
    console.log("===============Indexed========")
    return processBulkInserts(response);
};
