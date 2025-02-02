import { Client } from "@elastic/elasticsearch";
import { BulkStats } from "@elastic/elasticsearch/lib/helpers";


export const client = new Client({ 
    node: "http://localhost:9200",
    auth: {
        username: "elastic",
        password: "password"
    }
});

const processBulkInserts = (inserted: BulkStats) => {
    const total = inserted.total;
    const failed = inserted.failed;
    const successful= inserted.successful;

    console.log(`Total: ${total} Failed: ${failed} Successful: ${successful}`);

};

/**
 * Index a given array of documents in chunks of 250.
 *
 * @param index - The name of the index to use.
 * @param data - The array of documents to index.
 * @returns An array of promises that resolve when the respective chunk of documents has been indexed.
 */
export const indexBulk = async (index: string, data: any[]) => {
    if(data.length > 0){
        /*const body = data.flatMap(({id, ...doc}) => [
                { 
                    index: { 
                        _index: index, 
                        _id: id
                    } 
                },
                doc,
            ]
        );*/
        const response = await client.helpers.bulk({
            refresh: true,
            datasource: data,
            onDocument: (doc)=> ({ index: { _index: index, _id: doc.id } })
        });
        return processBulkInserts(response);
    }
    else{
        console.log("Nothing to index.")
    }
};
