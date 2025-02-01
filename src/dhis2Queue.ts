import { QueryDslQueryContainer } from "@elastic/elasticsearch/lib/api/types";
import axios from "axios";
import { Queue, Worker } from "bullmq";
import { OrgUnit } from "./interfaces";
import { layeringQueue } from "./layeringQueue";
import { connection } from "./redis";
import { processOrganisations, queryDHIS2Data } from "./utils";
import { layering2Queue } from "./layering2Queue";
import { layering3Queue } from "./layering3Queue";
import { logger } from "./logger";
import "dotenv/config"; 

export const dhis2Queue = new Queue<
    {
        program: string;
        generate: boolean;
        page?: number;
    } & Record<string, any>
>("dhis2", {
    connection,
});

const worker = new Worker<
    {
        program: string;
        generate: boolean;
        page?: number;
    } & Record<string, any>
>(
    "dhis2",
    async (job) => {
        let { page = 1, program, generate, ...others } = job.data;
        const api = axios.create({
            baseURL: process.env.DHIS2_URL,
            auth: {
                username: process.env.DHIS2_USERNAME ?? "",
                password: process.env.DHIS2_PASSWORD ?? "",
            },
            headers: {
                "Content-Type": "application/json",
            },
        });
        try {
            console.log("Fetching organisation units");
            const {
                data: { organisationUnits },
            } = await api.get<{
                organisationUnits: Array<OrgUnit>;
            }>("organisationUnits.json", {
                params: {
                    fields: "id,path,name,parent[name,parent[name]]",
                    paging: "false",
                    level: 5,
                },
            });
            const processedUnits = processOrganisations(organisationUnits);
            console.log("Querying DHIS2 for indexing");
            await queryDHIS2Data({
                program,
                page,
                processedUnits,
                api,
                ...others,
        /**
         * Callback to be executed when data is fetched from dhis2.
         * This callback is responsible for adding the data to the layering queues.
         * If the program is RDEklSXCD4C, it adds the data to the layering and layering3 queues.
         * If the program is lMC8XN5Lanc, it adds the data to the layering2 queue.
         * If the program is neither, it logs a message saying that the callback is not implemented.
         * @param data - array of tracked entity instance ids
         */
                callback: async (data: string[]) => {
                    if (
                        generate &&
                        data.length > 0 &&
                        program === "RDEklSXCD4C"
                    ) {
                        const query: QueryDslQueryContainer = {
                            terms: {
                                "trackedEntityInstance.keyword": data,
                            },
                        };
                        await layeringQueue.add(
                            String(new Date().getMilliseconds),
                            query,
                        );
                        await layering3Queue.add(
                            String(new Date().getMilliseconds),
                            query,
                        );
                    } 
                    else if (
                        generate &&
                        data.length > 0 &&
                        program === "lMC8XN5Lanc"
                    ) {
                        const query: QueryDslQueryContainer = {
                            terms: {
                                "trackedEntityInstance.keyword": data,
                            },
                        };
                        await layering2Queue.add(
                            String(new Date().getMilliseconds),
                            query,
                        );
                    }
                    else{
                        console.log("Not implemented");
                    }
                },
            });
        } catch (error) {
            console.log("Worker Error:",error);
        }
    },
    { connection },
);

worker.on("completed", (job) => {
    console.log(`${job.id} has completed!`);
    logger.info(`${job.id} has completed!`)
});

worker.on("failed", (job, err) => {
    console.log(`${job?.id} has failed with ${err.message}`);
    logger.info(`${job?.id} has failed with ${err.message}`);
});
