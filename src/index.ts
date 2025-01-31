import { QueryDslQueryContainer } from "@elastic/elasticsearch/lib/api/types";
import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { zValidator } from "@hono/zod-validator";
import axios from "axios";
import "dotenv/config";
import { Hono } from "hono";
import { cors } from "hono/cors";
import { trimTrailingSlash } from "hono/trailing-slash";
import { z } from "zod";
import { dhis2Queue } from "./dhis2Queue";
import { downloadQueue } from "./downloadQueue";
import { client } from "./elasticsearch";
import { generateXLS } from "./generateExcel";
import { instanceQueue } from "./instanceQueue";
import { layeringQueue } from "./layeringQueue";
import { scheduleQueue } from "./scheduleQueue";

const app = new Hono();

app.use("/*", cors());
app.use(trimTrailingSlash());
app.use("/static/*", serveStatic({ root: "./" }));

app.post(
    "/",
    zValidator(
        "json",
        z.object({
            trackedEntities: z.string().array().optional(),
        }),
    ),
    async (c) => {
        const options = c.req.valid("json");
        let query: QueryDslQueryContainer = {
            match_all: {},
        };
        if (options.trackedEntities) {
            query = {
                terms: {
                    "trackedEntityInstance.keyword": options.trackedEntities,
                },
            };
        }
        const job = await layeringQueue.add("myJobName", query);
        return c.json(job);
    },
);

app.get(
    "/download",
    zValidator(
        "query",
        z.object({
            period: z.string(),
            selectedOrgUnits: z.string(),
            code: z.string().optional(),
        }),
    ),
    async (c) => {
        const options = c.req.query();
        const response = await generateXLS({
            selectedOrgUnits: options.selectedOrgUnits.split(","),
            period: options.period,
            code: options.code,
        });
        return c.json(response);
    },
);

app.post(
    "/index",
    zValidator(
        "json",
        z.object({
            program: z.string(),
            page: z.number().optional(),
            others: z.record(z.any()),
            generate: z.boolean(),
        }),
    ),
    async (c) => {
        console.log("===Start indexing===")
        const { others, ...rest } = c.req.valid("json");
        const job = await dhis2Queue.add(rest.program, { ...rest, ...others });
        return c.json(job);
    },
);

app.post("/tei", async (c) => {
    const body = await c.req.json();
    const job = await downloadQueue.add("download", body);
    return c.json(job);
});
app.post("/sql", async (c) => {
    const query = await c.req.json();
    const data = await client.sql.query(query);
    return c.json(data);
});

app.post(
    "/schedule",
    zValidator(
        "json",
        z.object({
            lastUpdatedDuration: z.string(),
        }),
    ),
    async (c) => {
        const { lastUpdatedDuration } = c.req.valid("json");
        const job = await scheduleQueue.add(
            "scheduling",
            { lastUpdatedDuration },
            {
                repeat: { every: 1000 * 60 * 60 },
            },
        );
        return c.json(job);
    },
);

app.post(
    "/generate",
    zValidator(
        "json",
        z.object({
            lastUpdatedDuration: z.string().optional(),
        }),
    ),
    async (c) => {
        const { lastUpdatedDuration } = c.req.valid("json");
        const job = await scheduleQueue.add("running", {
            lastUpdatedDuration,
        });
        return c.json(job);
    },
);

app.get("/jobs", async (c) => {
    const removed = await scheduleQueue.removeRepeatableByKey(
        "schedule::::10000",
    );
    console.log(removed);
    const repeatableJobs = await scheduleQueue.getRepeatableJobs();

    return c.json(repeatableJobs);
});

app.post("/reset", async (c) => {
    const api = axios.create({
        baseURL: process.env.DHIS2_URL,
        auth: {
            username: process.env.DHIS2_USERNAME ?? "",
            password: process.env.DHIS2_PASSWORD ?? "",
        },
    });
    const {
        data: { programs },
    } = await api.get<{ programs: Array<{ id: string }> }>("programs.json", {
        params: { fields: "id", paging: false },
    });
    const {
        data: { programStages },
    } = await api.get<{ programStages: Array<{ id: string }> }>(
        "programStages.json",
        { params: { fields: "id", paging: false } },
    );

    const all = programs
        .concat(programStages)
        .map(({ id }) => String(id).toLowerCase());

    const links = all
        .concat("layering", "layering2", "layering3")
        .map((a) => `curl -X PUT localhost:9200/${a}?pretty`);

    for (const index of [...all, "layering", "layering2", "layering3"]) {
        console.log(`Working on ${index}`);
        try {
            await client.indices.delete({ index });
        } catch (error) {
            console.log(error);
        }
        try {
            await client.indices.create({
                index,
                settings: {
                    "index.mapping.total_fields.limit": "10000",
                },
            });
        } catch (error) {
            console.log(error);
        }
    }

    return c.json(links);
});

app.get(
    "/layer",
    zValidator(
        "query",
        z.object({
            instance: z.string(),
        }),
    ),
    async (c) => {
        const options = c.req.query();
        const job = await instanceQueue.add(options.instance, {
            username: process.env.DHIS2_USERNAME ?? "",
            password: process.env.DHIS2_PASSWORD ?? "",
            url: process.env.DHIS2_URL ?? "",
            instance: options.instance,
        });
        return c.json(job);
    },
);

const port = 3001;
console.log(`Server is running on port ${port}`);

serve({
    fetch: app.fetch,
    port,
});
