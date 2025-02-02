import { QueryDslQueryContainer } from "@elastic/elasticsearch/lib/api/types";
import { Queue, Worker } from "bullmq";

import dayjs from "dayjs";
import advancedFormat from "dayjs/plugin/advancedFormat";
import isoWeek from "dayjs/plugin/isoWeek";
import quarterOfYear from "dayjs/plugin/quarterOfYear";
import { connection } from "./redis";
import {
    scroll3,
    scroll,
    findAgeGroup,
    eventsWithinPeriod,
    scroll2,
} from "./utils";
import { indexBulk } from "./elasticsearch";
import sessions from "./sessions.json";
import { uniq } from "lodash";

dayjs.extend(isoWeek);
dayjs.extend(quarterOfYear);
dayjs.extend(advancedFormat);

export const layering2Queue = new Queue<QueryDslQueryContainer>("layering2", {
    connection,
});

const fetchData = async (trackedEntityInstances: any[]) => {
    const trackedEntityInstanceIds = trackedEntityInstances.map(
        (tei) => tei.trackedEntityInstance,
    );
    
    const allGroupActivitySessions = await scroll("VzkQBBglj3O", trackedEntityInstanceIds);
    const allGroupActivityBeneficiaries = await scroll("aTZwDRoJnxj", trackedEntityInstanceIds);
    const allOldGroupActivitySessions = await scroll("EVkAS8LJNbO", trackedEntityInstanceIds);
    return {
        allSessions: allGroupActivitySessions,
        allGroupActivityBeneficiaries,
        allOldGroupActivitySessions 
    };
};
const fetchActivities = async () => {
    const allSessions = await scroll2("IXxHJADVCkb");
    return allSessions.reduce((acc, session) => {
        acc[session["trackedEntityInstance"]] = session;
        return acc;
    },{});
};

const mapping2: any = {
    "GAT. Bank Linkages Sessions": 5,
    "GAT. Early Childhood Development Sessions": 8,
    "GAT. JOURNEYS PLUS (LARA) Sessions": 18,
    "GAT. MOH Journeys curriculum sessions": 22,
    "GAT. No means No sessions (Boys)": 4,
    "GAT. No means No sessions (Girls)": 4,
    "GAT. No means No sessions (Boys) New Curriculum": 8,
    "GAT. SINOVUYO Sessions": 14,
    "GAT. VSLA Saving and Borrowing": 6,
    "GAT. Financial Literacy Sessions": 4,
    "GAT. Group VSLA methodology sessions": 7,
    "GAT. SPM Training Sessions": 5,
    "GAT. VSLA TOT/Refresher Sessions": 3,
};

const generateLayering = (options: {
    trackedEntityInstances: any[];
    periods: dayjs.Dayjs[];
    allSessions: { [key: string]: any[] };
    activities: any;
    allGroupActivityBeneficiaries: { [key: string]: any[] };
    allOldGroupActivitySessions: { [key: string]: any[] };
}) => {
    const { trackedEntityInstances, allSessions, periods, activities } =
        options;
    let layering: any[] = [];

    const sessionMap = sessions.reduce<Record<string, string[]>>(
        (acc, session) => {
            acc[session.name] = session.options.map((o) => o.code);
            return acc;
        },
        {},
    );
    for (const {
        X4pNSt9UzOw,
        XzKmUgJRlRa,
        huFucxA3e5c, // name of beneficiary
        CfpoFtRmK1z, // sex
        N1nMqKtYKvI, // dob
        enrollmentDate,
        deleted,
        inactive,
        orgUnit,
        trackedEntityInstance,
        orgUnitName,
        district,
        subCounty,
        level1,
        level2,
        level3,
        level4,
        level5,
    } of trackedEntityInstances) {
        for (const period of periods) {
            const quarterStart = period.startOf("quarter");
            const quarterEnd = period.endOf("quarter");
            const qtr = period.format("YYYY[Q]Q");
            const id = `${trackedEntityInstance}${qtr}`;
            const {
                bFnIjGJpf9t,
                dqbuxC5GB1M,
                D7wRx9mgwns,
                mWyp85xIzXR,
                Pll79WEVWHj,
                oqabsHE0ZUI,
                Ah4eyDOBf51,
                cYDK0qZSri9,
                b76aEJUPnLy,
                jtpmu5rCeer,
            } = activities[XzKmUgJRlRa] ?? {
                bFnIjGJpf9t: "",
                dqbuxC5GB1M: "",
                D7wRx9mgwns: "",
                mWyp85xIzXR: "",
                Pll79WEVWHj: "",
                oqabsHE0ZUI: "",
                Ah4eyDOBf51: "",
                cYDK0qZSri9: "",
                b76aEJUPnLy: "",
                jtpmu5rCeer: "",
                XzKmUgJRlRa: "",
            };
            const age = period.diff(
                dayjs(N1nMqKtYKvI ? N1nMqKtYKvI : enrollmentDate),
                "years",
            );
            const ageGroup = findAgeGroup(age);

            const sessionsWithinQuarter = eventsWithinPeriod(
                sessions,
                quarterStart,
                quarterEnd,
            );
            let availableSessions: Record<string, number> = Object.values(
                sessionMap,
            )
                .flat()
                .reduce<Record<string, number>>(
                    (acc, code) => ({
                        ...acc,
                        [code]: 0,
                    }),
                    {},
                );
            let availableSessions2: Record<string, string[]> = {
                "GAT. Bank Linkages Sessions": [],
                "GAT. Early Childhood Development Sessions": [],
                "GAT. JOURNEYS PLUS (LARA) Sessions": [],
                "GAT. MOH Journeys curriculum sessions": [],
                "GAT. No means No sessions (Boys)": [],
                "GAT. No means No sessions (Girls)": [],
                "GAT. No means No sessions (Boys) New Curriculum": [],
                "GAT. SINOVUYO Sessions": [],
                "GAT. VSLA Saving and Borrowing": [],
                "GAT. Financial Literacy Sessions": [],
                "GAT. Group VSLA methodology sessions": [],
                "GAT. SPM Training Sessions": [],
                "GAT. VSLA TOT/Refresher Sessions": [],
            };
            sessionsWithinQuarter.forEach((session) => {
                const sessionCategory = session["qgikW8oSfNe"];
                const currentSessions =
                    session["ygHFm67aRqZ"]?.split(",") ?? [];

                if (sessionCategory === "1. VSLA Group") {
                    availableSessions2["GAT. Bank Linkages Sessions"] = [
                        ...(availableSessions2["GAT. Bank Linkages Sessions"] ??
                            []),
                        ...currentSessions.filter(
                            (s: string) =>
                                sessionMap[
                                    "GAT. Bank Linkages Sessions"
                                ].indexOf(s) !== -1,
                        ),
                    ];
                    availableSessions2["GAT. Financial Literacy Sessions"] = [
                        ...(availableSessions2[
                            "GAT. Financial Literacy Sessions"
                        ] ?? []),
                        ...currentSessions.filter(
                            (s: string) =>
                                sessionMap[
                                    "GAT. Financial Literacy Sessions"
                                ].indexOf(s) !== -1,
                        ),
                    ];
                    availableSessions2["GAT. Group VSLA methodology sessions"] =
                        [
                            ...(availableSessions2[
                                "GAT. Group VSLA methodology sessions"
                            ] ?? []),
                            ...currentSessions.filter(
                                (s: string) =>
                                    sessionMap[
                                        "GAT. Group VSLA methodology sessions"
                                    ].indexOf(s) !== -1,
                            ),
                        ];
                    availableSessions2["GAT. SPM Training Sessions"] = [
                        ...(availableSessions2["GAT. SPM Training Sessions"] ??
                            []),
                        ...currentSessions.filter(
                            (s: string) =>
                                sessionMap[
                                    "GAT. SPM Training Sessions"
                                ].indexOf(s) !== -1,
                        ),
                    ];
                    availableSessions2["GAT. VSLA Saving and Borrowing"] = [
                        ...(availableSessions2[
                            "GAT. VSLA Saving and Borrowing"
                        ] ?? []),
                        ...currentSessions.filter(
                            (s: string) =>
                                sessionMap[
                                    "GAT. VSLA Saving and Borrowing"
                                ].indexOf(s) !== -1,
                        ),
                    ];
                    availableSessions2["GAT. VSLA TOT/Refresher Sessions"] = [
                        ...(availableSessions2[
                            "GAT. VSLA TOT/Refresher Sessions"
                        ] ?? []),
                        ...currentSessions.filter(
                            (s: string) =>
                                sessionMap[
                                    "GAT. VSLA TOT/Refresher Sessions"
                                ].indexOf(s) !== -1,
                        ),
                    ];
                } else if (sessionCategory === "2. Sinovuyo") {
                    availableSessions2["GAT. SINOVUYO Sessions"] = [
                        ...(availableSessions2["GAT. SINOVUYO Sessions"] ?? []),
                        ...currentSessions.filter(
                            (s: string) =>
                                sessionMap["GAT. SINOVUYO Sessions"].indexOf(
                                    s,
                                ) !== -1,
                        ),
                    ];
                } else if (sessionCategory === "3. Journeys Plus") {
                    availableSessions2["GAT. JOURNEYS PLUS (LARA) Sessions"] = [
                        ...(availableSessions2[
                            "GAT. JOURNEYS PLUS (LARA) Sessions"
                        ] ?? []),
                        ...currentSessions.filter(
                            (s: string) =>
                                sessionMap[
                                    "GAT. JOURNEYS PLUS (LARA) Sessions"
                                ].indexOf(s) !== -1,
                        ),
                    ];
                    availableSessions2[
                        "GAT. MOH Journeys curriculum sessions"
                    ] = [
                        ...(availableSessions2[
                            "GAT. MOH Journeys curriculum sessions"
                        ] ?? []),
                        ...currentSessions.filter(
                            (s: string) =>
                                sessionMap[
                                    "GAT. MOH Journeys curriculum sessions"
                                ].indexOf(s) !== -1,
                        ),
                    ];
                } else if (sessionCategory === "4. NMN") {
                    availableSessions2["GAT. No means No sessions (Boys)"] = [
                        ...(availableSessions2[
                            "GAT. No means No sessions (Boys)"
                        ] ?? []),
                        ...currentSessions.filter(
                            (s: string) =>
                                sessionMap[
                                    "GAT. No means No sessions (Boys)"
                                ].indexOf(s) !== -1,
                        ),
                    ];
                    availableSessions2[
                        "GAT. No means No sessions (Boys) New Curriculum"
                    ] = [
                        ...(availableSessions2[
                            "GAT. No means No sessions (Boys) New Curriculum"
                        ] ?? []),
                        ...currentSessions.filter(
                            (s: string) =>
                                sessionMap[
                                    "GAT. No means No sessions (Boys) New Curriculum"
                                ].indexOf(s) !== -1,
                        ),
                    ];
                    availableSessions2["GAT. No means No sessions (Girls)"] = [
                        ...(availableSessions2[
                            "GAT. No means No sessions (Girls)"
                        ] ?? []),
                        ...currentSessions.filter(
                            (s: string) =>
                                sessionMap[
                                    "GAT. No means No sessions (Girls)"
                                ].indexOf(s) !== -1,
                        ),
                    ];
                } else if (
                    sessionCategory === "7. Early Childhood Development (ECD)"
                ) {
                    availableSessions2[
                        "GAT. Early Childhood Development Sessions"
                    ] = [
                        ...(availableSessions2[
                            "GAT. Early Childhood Development Sessions"
                        ] ?? []),
                        ...currentSessions.filter(
                            (s: string) =>
                                sessionMap[
                                    "GAT. Early Childhood Development Sessions"
                                ].indexOf(s) !== -1,
                        ),
                    ];
                } else if (sessionCategory === "5. Stepping Stones") {
                }
                currentSessions.forEach((v: string) => {
                    availableSessions[v] = 1;
                });
            });

            const availableSessions3 = Object.entries(
                availableSessions2,
            ).reduce<Record<string, number>>((acc, [key, value]) => {
                acc[key] = uniq(value).length;
                return acc;
            }, {});

            const availableSessions4 = Object.entries(
                availableSessions3,
            ).reduce<Record<string, number>>((acc, [key, value]) => {
                acc[`Completed ${key}`] = value >= mapping2[key] ? 1 : 0;
                return acc;
            }, {});

            layering.push({
                X4pNSt9UzOw,
                beneficiaryName: huFucxA3e5c,
                sex: CfpoFtRmK1z,
                dob: N1nMqKtYKvI,
                enrollmentDate,
                deleted,
                inactive,
                orgUnit,
                trackedEntityInstance,
                orgUnitName,
                district,
                subCounty,
                level1,
                level2,
                level3,
                level4,
                level5,
                ageGroup,
                age,
                qtr,
                id,
                bFnIjGJpf9t,
                dqbuxC5GB1M,
                D7wRx9mgwns,
                mWyp85xIzXR,
                Pll79WEVWHj,
                oqabsHE0ZUI,
                Ah4eyDOBf51,
                cYDK0qZSri9,
                b76aEJUPnLy,
                jtpmu5rCeer,
                XzKmUgJRlRa,
                ...availableSessions,
                ...availableSessions3,
                ...availableSessions4,
            });
        }
    }

    return layering;
};

const worker = new Worker<QueryDslQueryContainer>(
    "layering2",
    async (job) => {
        console.log("=============Starting Layering 2 Job ==============");
        try {
            const activities = await fetchActivities();
            await scroll3("RDEklSXCD4C", job.data, async (documents) => {
                const allData = await fetchData(documents);
                const layering = generateLayering({
                    ...allData,
                    periods: [
                        dayjs().subtract(6, "quarters"),
                        dayjs().subtract(5, "quarters"),
                        dayjs().subtract(4, "quarters"),
                        dayjs().subtract(3, "quarters"),
                        dayjs().subtract(2, "quarters"),
                        dayjs().subtract(1, "quarters"),
                        dayjs(),
                    ],
                    trackedEntityInstances: documents,
                    activities,
                });
                await indexBulk("layering2", layering);
            });
        }
        catch(e){
            console.log("Layering 2 worker error:",e)
        }
    },
    { connection },
);

worker.on("completed", (job) => {
    console.log(`Layering 2 job ${job.id} has completed!`);
});

worker.on("failed", (job, err) => {
    console.log(`Layering 2 job ${job?.id} has failed with ${err.message}`);
});
