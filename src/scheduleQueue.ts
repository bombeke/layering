import { Queue, Worker } from "bullmq";

import { QueryDslQueryContainer } from "@elastic/elasticsearch/lib/api/types";
import type { AxiosInstance } from "axios";
import { chunk, Dictionary } from "lodash";
import { layeringQueue } from "./layeringQueue";
import { connection } from "./redis";
import { processOrganisations, queryDHIS2Data } from "./utils";
import axios from "axios";
import { OrgUnit } from "./interfaces";
import { layering2Queue } from "./layering2Queue";

export const scheduleQueue = new Queue<{
    lastUpdatedDuration?: string;
}>("schedule", {
    connection,
});

const worker = new Worker<{
    lastUpdatedDuration?: string;
}>(
    "schedule",
    async (job: any) => {
        const { lastUpdatedDuration } = job.data;
        const api = axios.create({
            baseURL: process.env.DHIS2_URL,
            auth: {
                username: process.env.DHIS2_USERNAME ?? "",
                password: process.env.DHIS2_PASSWORD ?? "",
            },
        });
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

        let params: {
            program: string;
            page: number;
            processedUnits: Dictionary<{
                subCounty: string;
                district: string;
                orgUnitName: string;
            }>;
            ouMode: string;
            lastUpdatedDuration?: string;
            fields: string;
            api: AxiosInstance;
        } = {
            page: 1,
            ouMode: "ALL",
            fields: "*",
            program: "HEWq6yr4cs5",
            processedUnits,
            api,
        };
        if (lastUpdatedDuration) {
            params = { ...params, lastUpdatedDuration };
        }
        try {
            console.log("Fetching and Indexing HEWq6yr4cs5 program data");
            await queryDHIS2Data({
                ...params,
                program: "HEWq6yr4cs5",
            });
            console.log("Fetching and Indexing IXxHJADVCkb program data");

            await queryDHIS2Data({
                ...params,
                program: "IXxHJADVCkb",
            });

            console.log("Fetching and Indexing RDEklSXCD4C program data");

            await queryDHIS2Data({
                ...params,
                program: "RDEklSXCD4C",
                callback: (data: string[]) => {
                    if (data.length > 0) {
                        chunk(data, 250).map(async(c) =>{
                            const query: QueryDslQueryContainer = {
                                terms: {
                                    "trackedEntityInstance.keyword": c,
                                },
                            };
                            await layeringQueue.add(
                                String(new Date().getMilliseconds),
                                query,
                            );
                        });
                        
                        chunk(data, 250).map(async(c3) =>{
                            const query3: QueryDslQueryContainer = {
                                terms: {
                                    "trackedEntityInstance.keyword": c3,
                                },
                            };
                            // changed from 3 to 2
                            await layering2Queue.add(
                                String(new Date().getMilliseconds),
                                query3,
                            );
                        });
                    
                    }
                },
            });
        } catch (error) {
            console.log(error);
        }
    },
    { connection }
);

worker.on("completed", (job) => {
    console.log(`Scheduled Job ${job.id} has completed!`);
});

worker.on("failed", (job, err) => {
    console.log(`Scheduled Job ${job?.id} has failed with ${err.message}`);
});
