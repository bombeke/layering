import {
    QueryDslQueryContainer,
    SearchRequest,
} from "@elastic/elasticsearch/lib/api/types";
import { AxiosInstance } from "axios";
import dayjs, { Dayjs } from "dayjs";
import isBetween from "dayjs/plugin/isBetween";
import isSameOrBefore from "dayjs/plugin/isSameOrBefore";
import {
    chunk,
    Dictionary,
    every,
    fromPairs,
    groupBy,
    has,
    isEmpty,
    maxBy,
    minBy,
    orderBy,
    sum,
    uniq,
} from "lodash";
import { client, indexBulk } from "./elasticsearch";

import homeVisitSections from "./homeVisitSections.json";
import { OrgUnit } from "./interfaces";

dayjs.extend(isSameOrBefore);
dayjs.extend(isBetween);

const viralLoadStatuses: Record<string, string> = {
    "1": "Suppressed",
    "2": "Unsuppressed",
    "3": "Waiting",
    "4": "Rejected",
    "5": "Failed",
};

export function removeEmptyKeys(obj: { [key: string]: any }): {
    [key: string]: any;
} {
    return Object.entries(obj).reduce((acc, [key, value]) => {
        if (
            value !== null &&
            value !== undefined &&
            (typeof value !== "string" || value.trim() !== "")
        ) {
            acc[key] = typeof value === "string" ? value.trim() : value;
        }
        return acc;
    }, {} as { [key: string]: any });
}

export const eventsBeforePeriod = (events: any[], end: Dayjs) => {
    return events.filter((e) => {
        return dayjs(e.eventDate).isSameOrBefore(end);
    });
};

export const eventsWithinPeriod = (events: any[], start: Dayjs, end: Dayjs) => {
    const filtered = events.filter((e) => {
        return dayjs(e.eventDate).isBetween(start, end, "date", "[]");
    });
    return filtered;
};

export const mostCurrentEvent = (events: any[]) => {
    return maxBy(events, "eventDate");
};

export const getCurrentViralLoad = (viralLoads: any[], endDate: Dayjs) => {
    const virals = eventsBeforePeriod(viralLoads, endDate).filter(
        ({ Ti0huZXbAM0 }) => !!Ti0huZXbAM0,
    );
    let currentViralLoad = maxBy(
        virals,
        ({ Ti0huZXbAM0, eventDate }) => `${Ti0huZXbAM0}${eventDate}`,
    );

    if (!currentViralLoad && virals.length > 0) {
        currentViralLoad = maxBy(virals, ({ eventDate }) => eventDate);
    }
    return currentViralLoad;
};

export const currentEvent = (events: any[], endDate: Dayjs) => {
    const eventsBeforeEndDate = eventsBeforePeriod(events, endDate);
    return maxBy(eventsBeforeEndDate, "eventDate");
};

export const firstEvent = (events: any[], endDate: Dayjs) => {
    const eventsBeforeEndDate = eventsBeforePeriod(events, endDate);
    return minBy(eventsBeforeEndDate, "eventDate");
};
export const scroll = async (
    index: string,
    trackedEntityInstances: string[],
    columns?: string[],
) => {
    let query: SearchRequest = {
        index: index.toLowerCase(),
        query: {
            bool: {
                must: [
                    {
                        terms: {
                            "trackedEntityInstance.keyword":
                                trackedEntityInstances,
                        },
                    },
                    {
                        match: {
                            deleted: false,
                        },
                    },
                ],
            },
        },
        size: 1000,
    };

    if (columns) {
        query = { ...query, _source: columns };
    }
    const scrollSearch = client.helpers.scrollSearch(query);
    if (!scrollSearch || typeof scrollSearch[Symbol.asyncIterator] !== 'function') {
        console.log("scrollSearch is not iterable");
    }
    let documents: any[] = [];
    for await (const result of scrollSearch) {
        documents = documents.concat(result.documents);
    }
    return groupBy(documents, "trackedEntityInstance");  
};

export const scroll2 = async (index: string) => {
    let query: SearchRequest = {
        index: index.toLowerCase(),
        query: {
            match_all: {},
        },
        size: 1000,
    };
    const scrollSearch = client.helpers.scrollSearch(query);
    if (!scrollSearch || typeof scrollSearch[Symbol.asyncIterator] !== 'function') {
        console.log("scrollSearch is not iterable");
    }
    let documents: any[] = [];
    for await (const result of scrollSearch) {
        documents = documents.concat(result.documents);
    }
    return documents;
};

export const scroll4 = async (
    index: string,
    trackedEntityInstances: string[],
    columns?: string[],
) => {
    let query: SearchRequest = {
        index: index.toLowerCase(),
        query: {
            bool: {
              must: [
                {
                  match_all: {}
                }
              ],
              filter: [
                {
                  terms: {
                    "trackedEntityInstance.keyword": trackedEntityInstances
                  }
                }
              ]
            }
        },
        size: 1000,
    };

    if (columns) {
        query = { ...query, _source: columns };
    }
    const scrollSearch = client.helpers.scrollSearch(query);
    if (!scrollSearch || typeof scrollSearch[Symbol.asyncIterator] !== 'function') {
        console.log("scrollSearch is not iterable");
    }
    let documents: any[] = [];
    for await (const result of scrollSearch) {
        documents = documents.concat(result.documents);
    }
    return groupBy(documents, "trackedEntityInstance");
};


export const scroll3 = async (
    index: string,
    search: QueryDslQueryContainer,
    callback: (doc: any[]) => Promise<void>,
) => {
    try {
        if (!index || !search || !callback) {
            throw new Error("Invalid arguments passed to scroll3");
        }
        let query: SearchRequest = {
            index: index.toLowerCase(),
            query: search,
            size: 100,
        };

        const scrollSearch = client.helpers.scrollSearch(query);

        if (!scrollSearch || typeof scrollSearch[Symbol.asyncIterator] !== 'function') {
            console.log("scrollSearch is not iterable");
            await callback([]);
        }
        for await (const result of scrollSearch) {
            if (result && result.documents) {
                await callback(result.documents);
            } else {
                console.log("No documents found in the current scroll result");
                await callback([]);
            }
        }
    } 
    catch (error) {
        console.error("Scroll search failed:", error);
        await callback([]); // Ensure callback is still executed even on failure
    }
};


export const getEconomicStatus = (hvat: any) => {
    if (hvat) {
        const { zbAGBW6PsGd, kQCB9F39zWO, iRJUDyUBLQF } = hvat;

        const score18 = [zbAGBW6PsGd, kQCB9F39zWO, iRJUDyUBLQF].filter(
            (v) => v !== null && v !== undefined && v !== "",
        );
        const yeses = score18.filter((v) => v === "Yes").length;
        const noses = score18.filter((v) => v === "No").length;
        if (score18.length === 3) {
            if (noses === 3) {
                return "Destitute";
            }
            if (yeses === 3) {
                return "Ready to Grow";
            }
            if (noses >= 1) {
                return "Struggling";
            }
        }
    }
    return "";
};

export const latestEvent = (events: any[]) => {
    return maxBy(events, "eventDate");
};
export const baselineEvent = (events: any[]) => {
    return minBy(events, "eventDate");
};

export const getAttribute = (
    attribute: string,
    event?: { [key: string]: any },
) => {
    if (event) {
        return event[attribute] || "";
    }
    return "";
};

export const anyEventWithDE = (events: any[], dataElement: string) => {
    if (events.length === 0) {
        return false;
    }
    return (
        events.find((event) => {
            return has(event, dataElement) && event[dataElement];
        }) !== undefined
    );
};

export const findAnyEventValue = (events: any[], dataElement: string) => {
    const foundEvent = orderBy(events, ["eventDate"], ["asc"]).reduce(function (
        acc,
        x,
    ) {
        for (var key in x) acc[key] = x[key];
        return acc;
    },
    {});
    if (!isEmpty(foundEvent)) {
        return foundEvent[dataElement];
    }
    return "";
};

export const getAttributes = (
    attributes: string[],
    event?: { [key: string]: any },
) => {
    if (attributes) {
        return attributes.map((a) => getAttribute(a, event));
    }
    return [];
};
export const getMultiAttributes = (attributes: string[], events: any[]) => {
    return attributes.map((a) => findAnyEventValue(events, a));
};

export const getHEIInformation = (heiData: any[]) => {
    const attributes: string[] = [
        "sDMDb4InL5F",
        "aBc9Lr1z25H",
        "Qyp4adG3KJL",
        "yTSlwP6htQh",
        "fUY7DEjsZin",
        "TJPxuJHRA3P",
        "TX2qmTSj0rM",
        "r0zBP8h3UEl",
        "G0YhL0M4YjJ",
        "CWqTgshbDbW",
        "qitG6coAg3q",
        "lznDPbUscke",
        "fcAZR5zt9i3",
    ];

    const [
        eidEnrollmentDate,
        motherArtNo,
        eidNo,
        dateFirstPCRDone,
        firstPCRResults,
        dateSecondPCRDone,
        secondPCRResults,
        dateThirdPCRDone,
        thirdPCRResults,
        hivTestDueDate,
        dateHivTestDone,
        hivTestResults,
        finalOutcome,
    ] = getMultiAttributes(attributes, heiData);

    let pcr = "";

    if (hivTestResults || hivTestDueDate) {
        pcr = "4";
    } else if (thirdPCRResults || dateThirdPCRDone) {
        pcr = "3";
    } else if (secondPCRResults || dateSecondPCRDone) {
        pcr = "2";
    } else if (firstPCRResults || dateFirstPCRDone) {
        pcr = "1";
    }
    return {
        eidEnrollmentDate,
        motherArtNo,
        eidNo,
        dateFirstPCRDone,
        firstPCRResults:
            firstPCRResults === "1" ? "+" : firstPCRResults === "2" ? "-" : "",
        dateSecondPCRDone,
        secondPCRResults:
            secondPCRResults === "1"
                ? "+"
                : secondPCRResults === "2"
                ? "-"
                : "",
        dateThirdPCRDone,
        thirdPCRResults:
            thirdPCRResults === "1" ? "+" : thirdPCRResults === "2" ? "-" : "",
        hivTestDueDate,
        dateHivTestDone,
        hivTestResults:
            hivTestResults === "1" ? "+" : hivTestResults === "2" ? "-" : "",
        finalOutcome,
        pcr,
    };
};

export const getHIVStatus = ({
    HzUL8LTDPga,
    hivResult,
    hivTestResults,
    viralLoadsBe4Quarter,
    riskFactor,
}: Partial<{
    HzUL8LTDPga: string;
    hivResult: string;
    hivTestResults: string;
    viralLoadsBe4Quarter: any[];
    riskFactor: string;
}>) => {
    if (viralLoadsBe4Quarter && viralLoadsBe4Quarter.length > 0) {
        return "+";
    } else if (hivResult) {
        return hivResult === "Positive"
            ? "+"
            : hivResult === "Negative"
            ? "-"
            : "";
    } else if (hivTestResults) {
        return hivTestResults;
    } else if (riskFactor === "HEI") {
        return "DK";
    } else {
        if (HzUL8LTDPga === "Positive") {
            return "+";
        }
        if (HzUL8LTDPga === "Negative") {
            return "-";
        }
        if (HzUL8LTDPga === "Dont Know (DK)") {
            return "DK";
        }
        return "";
    }
};

export const getDataElement = (
    dataElement: string,
    event?: { [key: string]: any },
) => {
    if (event) return event[dataElement];
    return undefined;
};

export const calculateQuarter = (year: number, quarter: number) => {
    if (quarter === 1) {
        return [
            dayjs(new Date(`${year - 1}-10-01`)),
            dayjs(new Date(`${year}-03-31`)),
        ];
    }
    if (quarter === 2) {
        return [
            dayjs(new Date(`${year - 1}-10-01`)),
            dayjs(new Date(`${year}-06-30`)),
        ];
    }
    if (quarter === 3) {
        return [
            dayjs(new Date(`${year - 1}-10-01`)),
            dayjs(new Date(`${year}-09-30`)),
        ];
    }
    if (quarter === 4) {
        return [
            dayjs(new Date(`${year}-10-01`)),
            dayjs(new Date(`${year}-12-31`)),
        ];
    }
    return [dayjs(new Date(`${year}-10-01`)), dayjs(new Date(`${year}-12-31`))];
};

export const allEventsHaveSameValue = (
    events: any[],
    dataElement: string,
    value: any,
) => {
    if (events.length > 0) {
        return events.every((e) => e[dataElement] === value);
    }
    return true;
};

export const hasDataElementWithinPeriod = (
    events: any[],
    dataElement: string,
    value: any,
) => {
    return events.find((e) => e[dataElement] === value) !== undefined;
};

export const getNewlyPositive = ({
    newlyEnrolled,
    hivStatus,
    HzUL8LTDPga,
    previousViralLoads,
    previousReferrals,
}: {
    newlyEnrolled: boolean;
    hivStatus: string;
    HzUL8LTDPga: string;
    previousViralLoads: any[];
    previousReferrals: any[];
}) => {
    if (newlyEnrolled && hivStatus === "+") {
        return 1;
    }

    if (hivStatus === "+") {
        if (
            (HzUL8LTDPga === "Negative" || HzUL8LTDPga === "Dont Know (DK)") &&
            previousViralLoads.length === 0 &&
            (allEventsHaveSameValue(
                previousReferrals,
                "XTdRWh5MqPw",
                "Negative",
            ) ||
                allEventsHaveSameValue(
                    previousReferrals,
                    "XTdRWh5MqPw",
                    "Dont Know (DK)",
                ))
        ) {
            return 1;
        } else {
            return 0;
        }
    }
    return 0;
};

// TODO add art start date as base for
export const getNewlyTestedPositive = ({
    newlyReportedPositive,
    artStartDate,
    quarterStart,
    quarterEnd,
    referralsDuringYear,
    hivStatus,
}: {
    newlyReportedPositive: number;
    artStartDate: string;
    quarterStart: dayjs.Dayjs;
    quarterEnd: dayjs.Dayjs;
    referralsDuringYear: any[];
    hivStatus: string;
}) => {
    let newlyTestedPositive = 0;

    if (
        newlyReportedPositive === 1 &&
        artStartDate &&
        dayjs(artStartDate).isBetween(quarterStart, quarterEnd)
    ) {
        newlyTestedPositive = 1;
    } else if (
        newlyReportedPositive &&
        hasDataElementWithinPeriod(
            referralsDuringYear,
            "XTdRWh5MqPw",
            "Positive",
        )
    ) {
        newlyTestedPositive = 1;
    } else if (hivStatus === "+") {
        newlyTestedPositive = 0;
    }
    return newlyTestedPositive;
};

export const getNewlyTestedAndOnArt = ({
    newlyTestedPositive,
    onArt,
    serviceProvided,
}: {
    newlyTestedPositive: number;
    onArt: string;
    serviceProvided: string;
}) => {
    let newlyTestedAndOnArt = 0;
    if (newlyTestedPositive === 1 && onArt) {
        newlyTestedAndOnArt = 1;
    } else if (
        newlyTestedPositive &&
        serviceProvided === "Started HIV treatment"
    ) {
        newlyTestedAndOnArt = 1;
    }
    return newlyTestedAndOnArt;
};

export const anyEventHasDataElementValue = (
    events: any[],
    dataElement: string,
    value: any,
) => {
    return events.find((e) => e[dataElement] === value);
};

export const newlyTestedPositive = ({
    artStartDate,
    startDate,
    endDate,
    newlyPositive,
    referralsDuringYear,
}: {
    startDate: dayjs.Dayjs;
    endDate: dayjs.Dayjs;
    referralsDuringYear: any[];
    artStartDate?: string;
    newlyPositive?: number;
}) => {
    if (
        newlyPositive === 1 &&
        artStartDate &&
        dayjs(artStartDate).isBetween(startDate, endDate)
    ) {
        return 1;
    }
    if (
        newlyPositive &&
        anyEventHasDataElementValue(
            referralsDuringYear,
            "XTdRWh5MqPw",
            "Positive",
        )
    ) {
        return 1;
    }
    return 0;
};

export const checkRiskAssessment = (
    dataElements: string[],
    event?: { [key: string]: any },
    value?: string,
) => {
    if (event) {
        const de = dataElements
            .map((de) => event[de])
            .filter((v) => v !== undefined);
        if (de.length === 0) {
            return 0;
        }
        if (de.length < dataElements.length) {
            if (value && every(de, (v) => v === value)) {
                return 3;
            } else if (value && de.indexOf(value) !== -1) {
                return 2;
            }
            return 1;
        }
        if (de.length === dataElements.length) {
            if (value && every(de, (v) => v === value)) {
                return 6;
            } else if (value && de.indexOf(value) !== -1) {
                return 5;
            }
            return 4;
        }
    }
    return -1;
};

export const getRiskAssessment = (currentRiskAssessment?: {
    [key: string]: any;
}) => {
    const tbScreeningChild = checkRiskAssessment(
        ["DgCXKSDPTWn", "Rs5qrKay7Gq", "QEm2B8LZtzd", "X9n17I5Ibdf"],
        currentRiskAssessment,
    );
    const tbScreeningChild17 = checkRiskAssessment(
        [
            "DgCXKSDPTWn",
            "Rs5qrKay7Gq",
            "QEm2B8LZtzd",
            "X9n17I5Ibdf",
            "Oi6CUuucUCP",
        ],
        currentRiskAssessment,
    );
    const tbScreeningAdult = checkRiskAssessment(
        ["If8hDeux5XE", "ha2nnIeFgbu", "NMtrXN3NBqY", "Oi6CUuucUCP"],
        currentRiskAssessment,
    );

    const atTBRiskChild = checkRiskAssessment(
        ["DgCXKSDPTWn", "Rs5qrKay7Gq", "QEm2B8LZtzd", "X9n17I5Ibdf"],
        currentRiskAssessment,
        "true",
    );
    const atTBRiskChild17 = checkRiskAssessment(
        [
            "DgCXKSDPTWn",
            "Rs5qrKay7Gq",
            "QEm2B8LZtzd",
            "X9n17I5Ibdf",
            "Oi6CUuucUCP",
        ],
        currentRiskAssessment,

        "true",
    );
    const atTBRiskAdult = checkRiskAssessment(
        ["If8hDeux5XE", "ha2nnIeFgbu", "NMtrXN3NBqY", "Oi6CUuucUCP"],
        currentRiskAssessment,

        "true",
    );

    const notAtRisk = checkRiskAssessment(
        [
            "WlTMjkcP6gv",
            "Y8kX45XGXXI",
            "NN0M618qUFX",
            "MH5BGP1Ww2Q",
            "p3FSiLQ1q6T",
            "x1bL4w5EsPL",
            "dunvFwnbGQF",
            "oI9btGSwA7P",
        ],
        currentRiskAssessment,
        "false",
    );

    const notAtRiskAdult = checkRiskAssessment(
        [
            "WwMOTHl2cOz",
            "uf6tkJtuWpt",
            "zpvSpZxMYIV",
            "O6O0ADYLwua",
            "VOCmw7bULXR",
            "FHu4YfcrIQw",
            "Dny6B3ubQEa",
            "h7JCV3YLRJO",
            "VtnameiqmRy",
        ],
        currentRiskAssessment,
        "false",
    );

    return {
        notAtRisk,
        notAtRiskAdult,
        tbScreeningChild,
        tbScreeningChild17,
        tbScreeningAdult,
        atTBRiskChild,
        atTBRiskChild17,
        atTBRiskAdult,
    };
};

export const deHasAnyValue = (de: string, values: any[]) => {
    if (de && values.indexOf(de) !== -1) {
        return 1;
    }
    return 0;
};

export const findStatus = (currentHomeVisit: any) => {
    let {
        RVwtjzZO8qG: memberStatus = "No Home Visit",
        KyqXQlehtwG: householdStatus = "",
        yiKbqQvYunj: reasonForExit = "",
        Xy3kS6Jgd08: householdExitReason = "",
    } = currentHomeVisit || {};

    return {
        memberStatus,
        householdStatus,
        reasonForExit,
        householdExitReason,
    };
};

export const isAtSchool = (homeVisitValue: any, enrollmentValue: any) => {
    if (homeVisitValue) {
        return convertBoolToYesNo(homeVisitValue);
    }

    if (enrollmentValue === "Yes") {
        return "Y";
    }
    if (enrollmentValue === "No") {
        return "N";
    }

    return "NA";
};

export const hivInformation = ({
    artStartDate,
    quarterEnd,
    hivStatus,
    lastViralLoadDate,
    viralLoadResultsReceived,
    viralLoadCopies,
    viralLoadStatus,
    viralTestDone,
}: {
    artStartDate: string;
    hivStatus: string;
    quarterEnd: dayjs.Dayjs;
    lastViralLoadDate: string;
    viralTestDone: string;
    viralLoadResultsReceived: string;
    viralLoadCopies: string;
    viralLoadStatus: string;
}) => {
    let copies: string | undefined = "";
    let ovcEligible;
    let VLTestDone: number | string;
    let VLStatus;
    let ovcVL;
    let VLSuppressed;
    if (hivStatus === "+") {
        if (artStartDate) {
            const daysOnArt = quarterEnd.diff(dayjs(artStartDate), "months");
            if (daysOnArt >= 6) {
                ovcEligible = 1;
            } else if (lastViralLoadDate) {
                ovcEligible = 1;
            } else {
                ovcEligible = "NE";
            }
        } else if (lastViralLoadDate) {
            ovcEligible = 1;
        } else {
            ovcEligible = "No VL";
        }

        if (lastViralLoadDate && ovcEligible === 1) {
            const monthsSinceLastViralLoad = quarterEnd.diff(
                dayjs(artStartDate)!,
                "months",
            );
            if (monthsSinceLastViralLoad < 12) {
                VLTestDone =
                    viralTestDone === "true"
                        ? 1
                        : viralTestDone === "false"
                        ? 0
                        : 0;
                VLStatus = viralLoadStatus;
            } else {
                VLTestDone = 0;
            }
        } else {
            VLTestDone = 0;
        }
        if (viralLoadResultsReceived && VLTestDone === 1) {
            ovcVL = viralLoadResultsReceived === "true" ? 1 : 0;
            copies = viralLoadCopies;
        } else {
            ovcVL = 0;
        }
        if (ovcVL === 1) {
            VLSuppressed = viralLoadStatus === "Suppressed" ? 1 : 0;
        } else {
            ovcVL = 0;
            VLSuppressed = 0;
        }
    } else {
        VLTestDone = "";
        ovcEligible = "";
        ovcVL = "";
        VLStatus = "";
    }

    return {
        VLTestDone,
        ovcEligible,
        ovcVL,
        VLStatus,
        VLSuppressed,
        viralLoadCopies,
        copies,
    };
};

export const anyEventWithAnyOfTheValue = (
    events: any[],
    dataElement: string,
    values: string[],
) => {
    const search = events.find((event) => {
        return values.indexOf(event[dataElement]) !== -1;
    });

    if (search) return 1;
    return 0;
};

export const eventsHasDataElements = (
    events: any[],
    dataElements: string[],
) => {
    const cond = dataElements
        .map((element) => anyEventWithDE(events, element))
        .some((a) => a === true);

    if (cond) return 1;
    return 0;
};

export const eventsHasDataElementsWithValue = (
    events: any[],
    dataElements: string[],
    value: string,
) => {
    const cond = dataElements
        .map((element) => getAttribute(element, events))
        .some((a) => a === value);

    if (cond) return 1;
    return 0;
};

export const anyEventWithDataElement = (
    events: any[],
    dataElement: string,
    value: any,
) => {
    if (events.length === 0) {
        return undefined;
    }
    return events.find((event) => {
        return event[dataElement] === value;
    });
};

export const specificDataElement = (
    dataElement: string,
    event?: { [key: string]: any },
) => {
    return event ? event[dataElement] : null;
};

export const getIsNotAtRisk = (
    hivStatus: string,
    notAtRiskAdult: number,
    notAtRisk: number,
) => {
    let isNotAtRisk = 0;
    if (hivStatus !== "+") {
        if (
            [0, 3, 6].indexOf(notAtRiskAdult) !== -1 ||
            [0, 3, 6].indexOf(notAtRisk) !== -1
        ) {
            isNotAtRisk = 1;
        } else {
            isNotAtRisk = 0;
        }
    }

    return isNotAtRisk;
};

export const getUnknownStatus = ({
    hivStatus,
    riskFactor,
    isNotAtRisk,
    age,
    unknownOther,
}: {
    hivStatus: string;
    notAtRisk: number;
    riskFactor: string;
    isNotAtRisk: number;
    age: number;
    unknownOther: any;
}) => {
    let unknown = "";
    if (hivStatus !== "+" && hivStatus !== "-" && isNotAtRisk !== 1) {
        if (riskFactor === "HEI" && hivStatus === "DK" && age <= 2) {
            unknown = "HEI";
        } else if (unknownOther) {
            unknown = unknownOther;
        } else {
            unknown = "Other reasons";
        }
    }
};

// TODO add before art start date

export const monthsSinceViralTest = (
    comparisonDate: dayjs.Dayjs,
    numberOfMonths: number,
    viralLoadDate?: string,
) => {
    if (
        viralLoadDate &&
        comparisonDate.diff(dayjs(viralLoadDate), "months") <= numberOfMonths
    )
        return 1;

    return 0;
};
// TODO add 12 months
export const everMissed = (
    events: any[],
    dataElement: string,
    end: dayjs.Dayjs,
) => {
    return (
        events.filter((event) => {
            return (
                has(event, dataElement) &&
                has(event, "vnxQFpwvu67") &&
                event[dataElement] &&
                dayjs(event[dataElement]).isBefore(end) &&
                [
                    "CLHIV Identified but not yet returned to Care",
                    "Beneficiary Still Missing",
                    "Rescheduled appointment",
                ].indexOf(event["vnxQFpwvu67"]) !== -1
            );
        }).length > 0
    );
};

export const searchEventBe4DateDataElement = (
    events: any[],
    dataElement: string,
    end: dayjs.Dayjs,
) => {
    const filteredEvents = events.filter((event) => {
        return (
            has(event, dataElement) &&
            event[dataElement] &&
            dayjs(event[dataElement]).isBefore(end)
        );
    });

    return maxBy(
        filteredEvents,
        ({ [dataElement]: val, eventDate }) => `${val}${eventDate}`,
    );
};

export const missedAppointmentInfo = (
    missedAppointments: any[],
    quarterEnd: dayjs.Dayjs,
) => {
    let missedAppointmentDate = "";
    let missedAnAppointment = 0;
    let missedAnAppointmentReason = "";
    let missedAnAppointmentFollowupOutcome = "";
    let missedAnAppointmentAction = "";
    const latestMissedAppointment = searchEventBe4DateDataElement(
        missedAppointments,
        "XTl5dE2AcVM",
        quarterEnd,
    );

    const hasEverMissedAnAppointment = everMissed(
        missedAppointments,
        "XTl5dE2AcVM",
        quarterEnd,
    )
        ? 1
        : 0;
    if (
        latestMissedAppointment &&
        [
            "CLHIV Identified but not yet returned to Care",
            "Beneficiary Still Missing",
            "Rescheduled appointment",
        ].indexOf(latestMissedAppointment["vnxQFpwvu67"]) !== -1
    ) {
        missedAppointmentDate = latestMissedAppointment["XTl5dE2AcVM"] || "";
        missedAnAppointment = 1;
        missedAnAppointmentReason =
            latestMissedAppointment["UZWtGlGfNFq"] || "";
        missedAnAppointmentFollowupOutcome =
            latestMissedAppointment["vnxQFpwvu67"] || "";
        missedAnAppointmentAction =
            latestMissedAppointment["WiWI9KrxXLl"] || "";
    } else if (latestMissedAppointment) {
        missedAnAppointmentReason =
            latestMissedAppointment["UZWtGlGfNFq"] || "";
        missedAnAppointmentFollowupOutcome =
            latestMissedAppointment["vnxQFpwvu67"] || "";

        missedAnAppointmentAction =
            latestMissedAppointment["WiWI9KrxXLl"] || "";
    }

    return {
        missedAppointmentDate,
        missedAnAppointment,
        missedAnAppointmentReason,
        missedAnAppointmentFollowupOutcome,
        hasEverMissedAnAppointment,
        missedAnAppointmentAction,
    };
};

export const fetchGroupActivities4Instances = async (
    trackedEntityInstances: any[],
) => {
    const allMemberCodes = uniq(
        trackedEntityInstances.flatMap(({ HLKc2AKR9jW }) => {
            if (HLKc2AKR9jW) return HLKc2AKR9jW;
            return [];
        }),
    );
    if (allMemberCodes.length > 0) {
        let data: any[] = [];
        await scroll3(
            "VzkQBBglj3O",
            {
                terms: {
                    ["ypDUCAS6juy.keyword"]: allMemberCodes,
                },
            },
            async (response) => {
                data = data.concat(response);
            },
        );
        return groupBy(data, "ypDUCAS6juy");
    }

    return {};
};

export const anyService = (values: number[]) => {
    const hasAny = values.some((a) => a === 1);

    if (hasAny) return 1;
    return 0;
};

export const getSectionDataElements = (sectionId: string) => {
    const section = homeVisitSections.find((a) => a.id === sectionId);
    if (section) return section.dataElements.map((a) => a.id);
    return [];
};

export const processOrganisations = (organisationUnits: Array<OrgUnit>) => {
    return fromPairs(
        organisationUnits.map((unit) => {
            return [
                unit.id,
                {
                    subCounty: unit.parent ? unit.parent.name : "",
                    district: unit.parent
                        ? unit.parent.parent
                            ? unit.parent.parent.name
                            : ""
                        : "",
                    orgUnitName: unit.name,
                    ...fromPairs(
                        String(unit.path)
                            .split("/")
                            .slice(1)
                            .map((v, i) => {
                                return [`level${i + 1}`, v];
                            }),
                    ),
                },
            ];
        }),
    );
};

export const flattenInstances = (
    trackedEntityInstances: Array<any>,
    processedUnits: Record<string, any>,
) => {
    let instances = [];
    let calculatedEvents = [];
    for (const {
        trackedEntityInstance,
        orgUnit,
        attributes,
        enrollments,
        inactive,
        deleted,
        relationships,
    } of trackedEntityInstances) {
        const units = processedUnits[orgUnit];
        let processedAttributes = fromPairs(
            attributes.map(({ attribute, value }: any) => [attribute, value]),
        );
        const allRelations = relationships.reduce(
            (acc: Record<string, string>, current: any) => {
                const fromRel: string =
                    current.from?.trackedEntityInstance?.trackedEntityInstance;
                const toRel: string =
                    current.to?.trackedEntityInstance?.trackedEntityInstance;

                const type: string = current.relationshipType;

                if (fromRel && toRel && type) {
                    if (fromRel !== trackedEntityInstance) {
                        acc[type] = fromRel;
                    } else {
                        acc[type] = toRel;
                    }
                }
                return acc;
            },
            {},
        );
        if (enrollments.length > 0) {
            for (const {
                events,
                program,
                orgUnitName,
                enrollmentDate,
                incidentDate,
                attributes,
            } of enrollments) {
                processedAttributes = {
                    ...processedAttributes,
                    ...fromPairs(
                        attributes.map(({ attribute, value }: any) => [
                            attribute,
                            value,
                        ]),
                    ),
                };
                const instance = {
                    trackedEntityInstance,
                    id: trackedEntityInstance,
                    orgUnit,
                    ...processedAttributes,
                    ...allRelations,
                    ...units,
                    inactive,
                    deleted,
                    enrollmentDate,
                    incidentDate,
                    orgUnitName,
                    program,
                };
                instances.push(removeEmptyKeys(instance));
                if (events.length > 0) {
                    for (const {
                        dataValues,
                        dueDate,
                        eventDate,
                        event,
                        deleted,
                        notes,
                        relationships,
                        geometry,
                        ...eventDetails
                    } of events) {
                        if (eventDetails.status !== "SCHEDULE" && eventDate) {
                            const currentEvent = {
                                ...units,
                                ...fromPairs(
                                    dataValues.map(
                                        ({ dataElement, value }: any) => [
                                            dataElement,
                                            value,
                                        ],
                                    ),
                                ),
                                ...eventDetails,
                                id: event,
                                orgUnitName,
                                enrollmentDate,
                                incidentDate,
                                eventDate,
                                deleted,
                                event,
                            };
                            calculatedEvents.push(
                                removeEmptyKeys(currentEvent),
                            );
                        }
                    }
                }
            }
        }
    }

    return { calculatedEvents, instances };
};

export const insertData = async ({
    instances,
    calculatedEvents,
    program,
}: {
    instances: any[];
    calculatedEvents: any[];
    program: string;
}) => {
    const foundEvents = groupBy(calculatedEvents, "programStage");
    const requests = Object.entries(foundEvents || {}).flatMap(([stage, events]) => {
        if (events.length === 0) {
            return [];
        }
        else{
            return chunk(events, 1000).map((c) => indexBulk(stage.toLowerCase(), c));
        }
    });
    await Promise.all([
        ...chunk(instances || [], 1000).map((c) =>indexBulk(program.toLowerCase(), c)),
        ...requests,
    ]);
};

export const convertBoolToYesNo = (value: "true" | "false") => {
    if (value === "true") return "Y";
    if (value === "false") return "N";
    return "";
};
export const convertBoolToNum = (value: "true" | "false" | true | false) => {
    if (value === "true") return "1";
    if (value === "false") return "0";
    if (value) return "1";
    return "0";
};

export const getGraduationInfo = (
    mostRecentGraduation: any,
    quarterEnd: Dayjs,
) => {
    let preGraduated = 0;
    let fullyGraduated = 0;
    if (mostRecentGraduation) {
        const graduation = specificDataElement(
            "XPJtNCSNCdR",
            mostRecentGraduation,
        );
        if (graduation === "1") {
            preGraduated = 1;
        }
        const monthsSinceGraduation = quarterEnd.diff(
            dayjs(mostRecentGraduation.eventDate),
            "months",
        );

        if (monthsSinceGraduation >= 6 && preGraduated === 1) {
            fullyGraduated = 1;
        }
    }

    return { preGraduated, fullyGraduated };
};

export const getOVCInfo = ({
    newlyEnrolled,
    quarter,
    servedInPreviousQuarter,
    age,
    hivStatus,
    risks,
    riskFactor,
    notAtRisk,
    notAtRiskAdult,
    ovcVL,
}: {
    newlyEnrolled: boolean;
    quarter: number;
    servedInPreviousQuarter: number;
    age: number;
    notAtRisk: number;
    notAtRiskAdult: number;
    ovcVL: number | string;
    hivStatus: string;
    riskFactor: string;
    risks: any;
}) => {
    let OVC_SERV = 0;
    let OVC_ENROL = 0;
    if (newlyEnrolled && quarter === 1) {
        OVC_SERV = 1;
    } else if (quarter === 1 && servedInPreviousQuarter === 1) {
        OVC_SERV = 1;
    }

    if (
        (age <= 20 && ovcVL === 1) ||
        (String(ovcVL) === "1" && OVC_SERV === 1)
    ) {
        OVC_ENROL = 1;
    } else if (age <= 20 && hivStatus === "+") {
        OVC_ENROL = 0;
    }
    let OVC_SERV_SUBPOP = risks[riskFactor] || riskFactor;
    const OVC_HIV_STAT =
        hivStatus === "+" ||
        hivStatus === "-" ||
        ([0, 3, 6].indexOf(notAtRisk) !== -1 &&
            [0, 3, 6].indexOf(notAtRiskAdult) !== -1 &&
            hivStatus === "DK")
            ? 1
            : 0;

    return { OVC_ENROL, OVC_HIV_STAT, OVC_SERV, OVC_SERV_SUBPOP };
};

export const findAssetOwnership = (filtered: any[], quarterEnd: Dayjs) => {
    let assetOwnership = "Not Reassessed";

    const hVatsBeforePeriod = eventsBeforePeriod(filtered, quarterEnd);
    if (hVatsBeforePeriod.length > 1) {
        const elements = [
            "uhO8M5K9qIi",
            "CeugEZj51eF",
            "kSAAkvdbkhM",
            "cErI5PKyAHU",
            "OCpvfRcuwvz",
            "aEAF4v9lelU",
            "ncI6C5uZMfy",
            "wC0fo1gmoOy",
        ];
        const [current, previous] = hVatsBeforePeriod.slice(-2);
        const currentAssets = sum(elements.map((e) => Number(current[e] || 0)));
        const previousAssets = sum(
            elements.map((e) => Number(previous[e] || 0)),
        );
        if (currentAssets > previousAssets) {
            assetOwnership = "Improved";
        } else if (currentAssets < previousAssets) {
            assetOwnership = "Regressed";
        } else if (currentAssets === previousAssets) {
            assetOwnership = "Stationary";
        }
    }

    return assetOwnership;
};

export const getGraduationStatus = ({
    memberStatus,
    OVC_SERV,
    servedInPreviousQuarter,
    quarter,
    newlyEnrolled,
}: {
    memberStatus: string;
    OVC_SERV: number;
    servedInPreviousQuarter: number;
    quarter: number;
    newlyEnrolled: boolean;
}) => {
    let exitedWithGraduation = "";
    if (
        memberStatus === "Active" &&
        OVC_SERV === 0 &&
        servedInPreviousQuarter === 0 &&
        quarter === 0 &&
        newlyEnrolled
    ) {
        exitedWithGraduation = "Not served in both qtrs";
    } else if (OVC_SERV === 0 && quarter === 0 && memberStatus === "Active") {
        exitedWithGraduation = "Not served current qtr";
    } else if (
        OVC_SERV === 0 &&
        servedInPreviousQuarter === 0 &&
        memberStatus === "Active"
    ) {
        exitedWithGraduation = "Not served previous qtr";
    } else if (OVC_SERV === 0 && memberStatus === "No Home Visit") {
        exitedWithGraduation = "Not served in both qtrs";
    } else if (OVC_SERV === 0) {
        exitedWithGraduation = memberStatus;
    }
};

export const convertViralStatus = (status: string) => {
    return viralLoadStatuses[status] ?? "";
};

    /**
     * Query DHIS2 API for tracked entity instances based on given program
     * and other parameters. The function will iterate through the pages
     * of results and call the callback function with the new instances
     * after each page of results. After all pages have been processed,
     * the function will call insertData to save the instances to elastic
     * search.
     *
     * @param {object} options - options object
     * @param {string} options.program - program to query
     * @param {AxiosInstance} options.api - axios instance with configured
     *                                     base url and auth
     * @param {object} options.processedUnits - dictionary with sub_county
     *                                        as key and objects with
     *                                        subCounty, district and
     *                                        orgUnitName as value
     * @param {function} [options.callback] - function to call after each
     *                                        page of results
     * @param {*} options.otherParams - other parameters to pass to the
     *                                  api query
     */
export const queryDHIS2Data = async ({
    api,
    program,
    processedUnits,
    callback,
    ...otherParams
}: {
    program: string;
    api: AxiosInstance;
    processedUnits: Dictionary<{
        subCounty: string;
        district: string;
        orgUnitName: string;
    }>;
    callback?: (instances: string[]) => void;
} & Record<string, any>) => {
    const maxPages = 150;
    const instances: string[] = [];
    const calculatedEvents: any[] = [];

    let totalPageCount = 1;

    for (let pageCount = 1; pageCount <= Math.min(totalPageCount, maxPages); pageCount++) {
        const params: any = {
            ...otherParams,
            page: pageCount,
            program,
        };
        if (pageCount === 1) {
            params.totalPages = true;
        }
        console.log(`Fetching ${ program } data for page ${pageCount} of ${totalPageCount}`);
        const {
            data: { 
                trackedEntityInstances, 
                pager 
            },
        } = await api.get<{
            trackedEntityInstances: Array<any>;
            pager: { 
                pageCount: number 
            };
        }>("trackedEntityInstances.json", { params });

        if (pageCount === 1 && pager.pageCount) {
            totalPageCount = pager.pageCount;
        }
        if (trackedEntityInstances.length > 0) {
            const { instances: newInstances, calculatedEvents: newEvents } = flattenInstances(
                trackedEntityInstances,
                processedUnits,
            );
            instances.push(...(newInstances as any));
            calculatedEvents.push(...newEvents);
        }
    }
    console.log("Processing instances: ",instances.length, " for ", program);
    if (instances.length > 0) {
        await insertData({ instances, calculatedEvents, program });
        if (callback) {
            callback(instances);
        }
    }
    else{
        console.log("No instances found to index");
    }
};


export const getGraduationAssessment = (currentGraduationAssessment: any) => {
    const graduationAssessment = [
        "QzuE1EG64rC",
        "suCGOqF6Q0N",
        "MKVqOBL22kp",
        "PW4BPCQ3RyX",
        "Hh9Zq9nX9uS",
        "lBpEEsJTWQR",
        "oJnCG2SxvL9",
        "sfM2HpxKYK4",
    ].flatMap((de) => {
        if (currentGraduationAssessment) {
            if (currentGraduationAssessment[de] === "Not Applicable")
                return "Met";

            return currentGraduationAssessment[de];
        }
        return [];
    });

    const graduationAssessmentScore = graduationAssessment.filter(
        (a) => a === "Met",
    ).length;

    if (graduationAssessmentScore === 8) {
        return { preGraduated: 1, graduated: 1 };
    }
    if (graduationAssessmentScore >= 6) {
        return { preGraduated: 1, graduated: 0 };
    }
    return { preGraduated: 0, graduated: 0 };
};

export const convertSearchTo10 = (search: any) => {
    if (search) return 1;
    return 0;
};

export const findAgeGroup = (age: number) => {
    if (age <= 0) {
        return "< 1";
    }

    if (age > 0 && age <= 4) {
        return "1 - 4";
    }
    if (age > 4 && age <= 9) {
        return "5 - 9";
    }
    if (age > 9 && age <= 14) {
        return "10 - 14";
    }
    if (age > 14 && age <= 17) {
        return "15 - 17";
    }
    if (age > 17 && age <= 20) {
        return "18 - 20";
    }
    if (age > 20 && age <= 24) {
        return "21 - 24";
    }
    if (age >= 25) {
        return "25+";
    }
};
