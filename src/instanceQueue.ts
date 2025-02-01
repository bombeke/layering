import axios from "axios";
import { Queue, Worker } from "bullmq";
import { OrgUnit } from "./interfaces";
import { connection } from "./redis";
import { flattenInstances, insertData, processOrganisations } from "./utils";
import { QueryDslQueryContainer } from "@elastic/elasticsearch/lib/api/types";
import { layeringQueue } from "./layeringQueue";

export const instanceQueue = new Queue<{
    username: string;
    password: string;
    url: string;
    instance: string;
}>("instance", {
    connection,
});

const worker = new Worker<{
    username: string;
    password: string;
    url: string;
    instance: string;
}>(
    "instance",
    async (job) => {
        let { instance, url, username, password } = job.data;
        const api = axios.create({
            baseURL: url,
            auth: { username: username, password: password },
        });
        try {
            console.log("Fetching instance data");
            const { data } = await api.get(
                `trackedEntityInstances/${instance}.json`,
                {
                    params: {
                        fields: "*",
                    },
                }
            );
            const unit = data.enrollments[0].orgUnit;
            const program = data.enrollments[0].program;
            const { data: orgUnit } = await api.get<OrgUnit>(
                `organisationUnits/${unit}.json`,
                {
                    params: {
                        fields: "id,path,name,parent[name,parent[name]]",
                    },
                }
            );

            const processedUnits = processOrganisations([orgUnit]);
            const { instances, calculatedEvents } = flattenInstances(
                [data],
                processedUnits
            );
            await insertData({ instances, calculatedEvents, program });

            let query: QueryDslQueryContainer = {
                term: {
                    "trackedEntityInstance.keyword": instance,
                },
            };

            console.log("Adding job to layering queue");
            const job = await layeringQueue.add(instance, query);
            return job;
        } 
        catch (error: any) {
            console.log("Instance worker error: ",error?.message);
            return error;
        }
    },
    { connection }
);

worker.on("completed", (job) => {
    console.log(`Instance job ${job.id} has completed!`);
});

worker.on("failed", (job, err) => {
    console.log(`Instance job ${job?.id} has failed with ${err.message}`);
});
