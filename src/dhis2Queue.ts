import { QueryDslQueryContainer } from "@elastic/elasticsearch/lib/api/types";
import axios from "axios";
import { Queue, Worker } from "bullmq";
import { OrgUnit } from "./interfaces";
import { layeringQueue } from "./layeringQueue";
import { connection } from "./redis";
import { processOrganisations, queryDHIS2Data } from "./utils";
import { layering2Queue } from "./layering2Queue";
import { layering3Queue } from "./layering3Queue";
import "dotenv/config"; 
import { chunk } from "lodash";

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
        console.log("=============Starting DHIS2 Job ==============");
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
            if( program === "HEWq6yr4cs5"){
                console.log("=== Starting indexing for HouseHold Program ===");
                await queryDHIS2Data({
                    ...others,
                    page,
                    processedUnits,
                    api,
                    program: "HEWq6yr4cs5",
                });
            }
            else{
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
                        console.log("Adding data to layering queues: ",data.length," items for program - ",program);
                        if (
                            generate &&
                            data.length > 0 &&
                            program === "RDEklSXCD4C"
                        ) {
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
                                await layering3Queue.add(
                                    String(new Date().getMilliseconds),
                                    query3,
                                );
                            });
                        
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
            }
        } 
        catch (error) {
            console.log("DHIS2 queue worker error:",error);
        }
    },
    { connection },
);

worker.on("completed", (job) => {
    console.log(`DHIS2 Job ${job.id} has completed!`);
});

worker.on("failed", (job, err) => {
    console.log(`DHIS2 Job ${job?.id} has failed with ${err.message}`);
});
