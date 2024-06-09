import { Queue, Worker } from "bullmq";
import dayjs from "dayjs";
import advancedFormat from "dayjs/plugin/advancedFormat";
import isoWeek from "dayjs/plugin/isoWeek";
import quarterOfYear from "dayjs/plugin/quarterOfYear";
import { Dictionary, fromPairs, orderBy, uniq, uniqBy } from "lodash";
import { indexBulk } from "./elasticsearch";
import { connection } from "./redis";
import {
    anyEventHasDataElementValue,
    anyEventWithAnyOfTheValue,
    anyEventWithDE,
    anyEventWithDataElement,
    anyService,
    baselineEvent,
    calculateQuarter,
    convertBoolToNum,
    convertBoolToYesNo,
    deHasAnyValue,
    eventsBeforePeriod,
    eventsHasDataElements,
    eventsWithinPeriod,
    fetchGroupActivities4Instances,
    findAnyEventValue,
    findAssetOwnership,
    findStatus,
    getAttribute,
    getAttributes,
    getDataElement,
    getEconomicStatus,
    getGraduationInfo,
    getGraduationStatus,
    getHEIInformation,
    getHIVStatus,
    getIsNotAtRisk,
    getMultiAttributes,
    getNewlyPositive,
    getNewlyTestedAndOnArt,
    getNewlyTestedPositive,
    getOVCInfo,
    getRiskAssessment,
    getSectionDataElements,
    getUnknownStatus,
    hivInformation,
    isAtSchool,
    latestEvent,
    missedAppointmentInfo,
    monthsSinceViralTest,
    scroll,
    scroll3,
} from "./utils";
import { QueryDslQueryContainer } from "@elastic/elasticsearch/lib/api/types";

dayjs.extend(isoWeek);
dayjs.extend(quarterOfYear);
dayjs.extend(advancedFormat);

export const myQueue = new Queue<QueryDslQueryContainer>("query", {
    connection,
});

const risks = {
    "Child of Non suppressed HIV+ Caregiver": "Child of HIV+ Caregiver",
    "Child of suppressed HIV+ Caregiver": "Child of HIV+ Caregiver",
    "Adolescent (9-14 yrs)": "Siblings of Index Child",
    "Malnourished (0-5 Yrs)": "Siblings of Index Child",
};

const processPreviousLayering = (layering: Dictionary<any[]>) => {
    return fromPairs(
        Object.entries(layering).map(([instance, data]) => [
            instance,
            fromPairs(
                data.map((d) => [
                    d["qtr"],
                    {
                        fullyGraduated: d["fullyGraduated"],
                        quarter: d["quarter"],
                    },
                ])
            ),
        ])
    );
};

const previousLayering = async (trackedEntityInstances: string[]) => {
    const layering = await scroll("layering", trackedEntityInstances, [
        "trackedEntityInstance",
        "qtr",
        "quarter",
        "fullyGraduated",
        "preGraduated",
    ]);
    return processPreviousLayering(layering);
};

const getEvents = (
    available: { [key: string]: any[] },
    trackedEntityInstance: string
) => {
    return available[trackedEntityInstance] || [];
};

const fetchData = async (trackedEntityInstances: any[]) => {
    const trackedEntityInstanceIds = trackedEntityInstances.map(
        (tei) => tei.trackedEntityInstance
    );
    const allInstances = uniq(
        trackedEntityInstances.map(({ hly709n51z0 }) => hly709n51z0)
    ).filter((v) => !!v);
    const previousLayer = await previousLayering(trackedEntityInstanceIds);
    const allHomeVisits = await scroll("HaaSLv2ur0l", trackedEntityInstanceIds);
    const allHivRiskAssessments = await scroll(
        "B9EI27lmQrZ",
        trackedEntityInstanceIds
    );
    const allViralLoads = await scroll("kKlAyGUnCML", trackedEntityInstanceIds);
    const allReferrals = await scroll("yz3zh5IFEZm", trackedEntityInstanceIds);
    const allServiceLinkages = await scroll(
        "SxnXrDtSJZp",
        trackedEntityInstanceIds
    );
    const allHVATAssessments = await scroll("sYE3K7fFM4Y", allInstances);
    const indexCases = await scroll("HEWq6yr4cs5", allInstances);
    const allGraduationAssessments = await scroll(
        "Cx35Elpu330",
        allInstances,

        ["trackedEntityInstance,eventDate,XPJtNCSNCdR"]
    );
    const allMissedAppointments = await scroll(
        "qNxRoC1wIYA",
        trackedEntityInstanceIds
    );
    const allDirectBeneficiaries = await scroll(
        "Hg8f2oHcKvH",
        trackedEntityInstanceIds
    );
    const allGroupActivities = await fetchGroupActivities4Instances(
        trackedEntityInstances
    );
    const allProtectionFunds = await scroll(
        "pyT5GQSyHZG",
        trackedEntityInstanceIds
    );
    const allIncomeGeneratingActivities = await scroll(
        "yF0ujXkemkw",
        trackedEntityInstanceIds
    );
    const allSchoolMappings = await scroll(
        "zAaPkLNSWwL",
        trackedEntityInstanceIds
    );
    const allSchoolMonitoring = await scroll(
        "NwazREY6KZM",
        trackedEntityInstanceIds
    );
    const allGBVScreening = await scroll(
        "SGChaH7CoMA",
        trackedEntityInstanceIds
    );
    return {
        allHomeVisits,
        allHivRiskAssessments,
        allViralLoads,
        allReferrals,
        allServiceLinkages,
        allHVATAssessments,
        allGraduationAssessments,
        allMissedAppointments,
        indexCases,
        previousLayering: previousLayer,
        allDirectBeneficiaries,
        allGroupActivities,
        allProtectionFunds,
        allIncomeGeneratingActivities,
        allSchoolMappings,
        allSchoolMonitoring,
        allGBVScreening,
    };
};

const findAgeGroup = (age: number) => {
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

const generateLayering = (options: {
    trackedEntityInstances: any[];
    periods: dayjs.Dayjs[];
    indexCases: { [key: string]: any[] };
    previousLayering: { [key: string]: any };
    allHomeVisits: { [key: string]: any[] };
    allHivRiskAssessments: { [key: string]: any[] };
    allViralLoads: { [key: string]: any[] };
    allReferrals: { [key: string]: any[] };
    allServiceLinkages: { [key: string]: any[] };
    allHVATAssessments: { [key: string]: any[] };
    allGraduationAssessments: { [key: string]: any[] };
    allMissedAppointments: { [key: string]: any[] };
    allDirectBeneficiaries: { [key: string]: any[] };
    allGroupActivities: { [key: string]: any[] };
    allProtectionFunds: { [key: string]: any[] };
    allIncomeGeneratingActivities: { [key: string]: any[] };
    allSchoolMappings: { [key: string]: any[] };
    allSchoolMonitoring: { [key: string]: any[] };
    allGBVScreening: { [key: string]: any[] };
}) => {
    const {
        trackedEntityInstances,
        periods,
        indexCases,
        previousLayering,
        allHomeVisits,
        allHivRiskAssessments,
        allViralLoads,
        allReferrals,
        allServiceLinkages,
        allHVATAssessments,
        allGraduationAssessments,
        allMissedAppointments,
        allDirectBeneficiaries,
        allGroupActivities,
        allProtectionFunds,
        allIncomeGeneratingActivities,
        allSchoolMappings,
        allSchoolMonitoring,
        allGBVScreening,
    } = options;

    let layering: any[] = [];
    for (const {
        enrollmentDate,
        hly709n51z0,
        HLKc2AKR9jW,
        N1nMqKtYKvI,
        nDUbdM2FjyP,
        h4pXErY01YR,
        umqeJCVp4Zq,
        HzUL8LTDPga,
        tHCT4RKXoiU,
        e0zEpTw7IH6,
        huFucxA3e5c,
        CfpoFtRmK1z,
        n7VQaJ8biOJ,
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
        const homeVisits = getEvents(allHomeVisits, trackedEntityInstance);
        const hivRiskAssessments = getEvents(
            allHivRiskAssessments,
            trackedEntityInstance
        );
        const viralLoads = getEvents(allViralLoads, trackedEntityInstance);
        const referrals = getEvents(allReferrals, trackedEntityInstance);
        const serviceLinkages = getEvents(
            allServiceLinkages,
            trackedEntityInstance
        );
        const missedAppointments = getEvents(
            allMissedAppointments,
            trackedEntityInstance
        );
        const directBeneficiaries = getEvents(
            allDirectBeneficiaries,
            trackedEntityInstance
        );
        const protectionFunds = getEvents(
            allProtectionFunds,
            trackedEntityInstance
        );
        const incomeGeneratingActivities = getEvents(
            allIncomeGeneratingActivities,
            trackedEntityInstance
        );
        const schoolMappings = getEvents(
            allSchoolMappings,
            trackedEntityInstance
        );
        const schoolMonitoring = getEvents(
            allSchoolMonitoring,
            trackedEntityInstance
        );
        const GBVScreenings = getEvents(allGBVScreening, trackedEntityInstance);
        const hasEnrollment = !!enrollmentDate;
        let mostRecentGraduation = {};
        const HVATAssessments = allHVATAssessments[hly709n51z0] || [];

        if (
            allGraduationAssessments[hly709n51z0] &&
            allGraduationAssessments[hly709n51z0] !== undefined
        ) {
            const filtered = orderBy(
                allGraduationAssessments,
                ["eventDate"],
                ["desc"]
            );
            if (filtered.length > 0) {
                mostRecentGraduation = filtered[0];
            }
        }

        const {
            Xkwy5P2JG24 = "",
            ExnzeYjgIaT = "",
            IyKRQFkfwMk = "",
            r10igcWrpoH = "",
        } = indexCases
            ? indexCases[hly709n51z0] && indexCases[hly709n51z0].length > 0
                ? indexCases[hly709n51z0][0]
                : {}
            : {};

        let allPreviousLayering = previousLayering[trackedEntityInstance] || {};

        const dob = N1nMqKtYKvI;
        let currentLayer = {
            trackedEntityInstance,
            beneficiaryId: HLKc2AKR9jW,
            e0zEpTw7IH6,
            tHCT4RKXoiU,
            enrollmentDate,
            type: "Comprehensive",
            district,
            subCounty,
            parish: orgUnitName,
            village: Xkwy5P2JG24,
            IyKRQFkfwMk,
            householdHead: ExnzeYjgIaT,
            beneficiaryName: huFucxA3e5c,
            N1nMqKtYKvI,
            sex: CfpoFtRmK1z,
            umqeJCVp4Zq,
            householdCode: r10igcWrpoH,
            level1,
            level2,
            level3,
            level4,
            level5,
        };

        const uniqHVATAssessments = uniqBy(HVATAssessments, "eventDate");

        const filtered = orderBy(
            uniqHVATAssessments.filter((e) => e.eventDate),
            ["eventDate"],
            ["desc"]
        );

        for (const period of periods) {
            const quarterStart = period.startOf("quarter");
            const quarterEnd = period.endOf("quarter");
            const qtr = period.format("YYYY[Q]Q");
            const [financialQStart, financialQEnd] = calculateQuarter(
                quarterStart.year(),
                period.quarter()
            );
            const id = `${trackedEntityInstance}${qtr}`;
            const age = period.diff(dayjs(dob), "years");
            const ageGroup = findAgeGroup(age);
            const previousQuarter = quarterStart
                .subtract(1, "quarters")
                .format("YYYY[Q]Q");
            const viralLoadsB4Quarter = eventsBeforePeriod(
                viralLoads,
                quarterEnd
            );
            const homeVisitsB4Quarter = eventsBeforePeriod(
                homeVisits,
                quarterEnd
            );

            const directBeneficiariesB4Quarter = eventsBeforePeriod(
                directBeneficiaries,
                quarterEnd
            );
            const incomeGeneratingActivitiesB4Quarter = eventsBeforePeriod(
                incomeGeneratingActivities,
                quarterEnd
            );
            const schoolMappingsB4Quarter = eventsBeforePeriod(
                schoolMappings,
                quarterEnd
            );
            const schoolMonitoringB4Quarter = eventsBeforePeriod(
                schoolMonitoring,
                quarterEnd
            );
            const protectionFundsB4Quarter = eventsBeforePeriod(
                protectionFunds,
                quarterEnd
            );

            // const referralsDuringYear = eventsWithinPeriod(
            //     referrals,
            //     financialQStart,
            //     financialQEnd
            // );

            const riskAssessmentsDuringQuarter = eventsWithinPeriod(
                hivRiskAssessments,
                quarterStart,
                quarterEnd
            );

            const referralsDuringQuarter = eventsWithinPeriod(
                referrals,
                quarterStart,
                quarterEnd
            );
            const serviceLinkagesDuringQuarter = eventsWithinPeriod(
                serviceLinkages,
                quarterStart,
                quarterEnd
            );
            const homeVisitsDuringQuarter = eventsWithinPeriod(
                homeVisits,
                quarterStart,
                quarterEnd
            );
            const viralLoadDuringQuarter = eventsWithinPeriod(
                viralLoads,
                quarterStart,
                quarterEnd
            );
            const protectionFundsDuringQuarter = eventsWithinPeriod(
                protectionFunds,
                quarterStart,
                quarterEnd
            );
            const GBVScreeningDuringQuarter = eventsWithinPeriod(
                GBVScreenings,
                quarterStart,
                quarterEnd
            );

            const currentViralLoad = latestEvent(viralLoadsB4Quarter);
            const currentHomeVisit = latestEvent(homeVisitsB4Quarter);
            const currentDirectBeneficiary = latestEvent(
                directBeneficiariesB4Quarter
            );
            const currentReferral = latestEvent(referralsDuringQuarter);
            const currentRiskAssessment = latestEvent(
                riskAssessmentsDuringQuarter
            );
            const currentHVAT = latestEvent(HVATAssessments);
            const firstHVAT = baselineEvent(HVATAssessments);
            const firstHomeVisit = baselineEvent(homeVisitsB4Quarter);
            const currentSchoolMapping = latestEvent(schoolMappingsB4Quarter);

            const previousViralLoads = eventsBeforePeriod(
                viralLoads,
                quarterStart
            );
            const previousReferrals = eventsBeforePeriod(
                referrals,
                quarterStart
            );

            let eventDate = "";

            if (currentHVAT) {
                eventDate = currentHVAT.eventDate;
            }
            const newlyEnrolled = dayjs(enrollmentDate).isBetween(
                quarterStart,
                quarterEnd
            );

            let riskFactor = findAnyEventValue(
                homeVisitsB4Quarter,
                "rQBaynepqjy"
            );
            const otherRiskFactor = findAnyEventValue(
                homeVisitsB4Quarter,
                "V7oko4Tm3N8"
            );

            const baselineRiskFactor =
                nDUbdM2FjyP || firstHomeVisit?.["rQBaynepqjy"];
            const reasonForExit = currentHomeVisit?.["yiKbqQvYunj"] ?? "";
            const reasonForVisit = currentHomeVisit?.["t7kVwlLVBns"] ?? "";
            const householdExitReason = currentHomeVisit?.["Xy3kS6Jgd08"] ?? "";
            const VSLASavings = currentDirectBeneficiary?.["H5vsW6LYFhy"] ?? "";
            const VSLABorrowing =
                currentDirectBeneficiary?.["s4w6hTytt5h"] ?? "";

            const [
                artStartDate,
                weight,
                lastViralLoadDate,
                viralTestDone,
                viralLoadResultsReceived,
                viralLoadStatus,
                viralLoadCopies,
                regimen,
            ] = getAttributes(
                [
                    "epmIBD8gh7G",
                    "Kjtt7SV26zL",
                    "Ti0huZXbAM0",
                    "cM7dovIX2Dl",
                    "te2VwealaBT",
                    "AmaNW7QDuOV",
                    "b8p0uWaYRhY",
                    "nZ1omFVYFkT",
                    "usRWNcogGX7",
                ],
                currentViralLoad
            );

            const hivResult = getDataElement("XTdRWh5MqPw", currentReferral);

            const {
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
                pcr,
            } = getHEIInformation(viralLoadsB4Quarter);

            const hivStatus = getHIVStatus({
                HzUL8LTDPga,
                viralLoadsBe4Quarter: viralLoadsB4Quarter,
                hivResult,
                riskFactor,
                hivTestResults,
            });

            const newlyPositive = getNewlyPositive({
                newlyEnrolled,
                hivStatus,
                HzUL8LTDPga,
                previousReferrals,
                previousViralLoads,
            });

            const newlyTestedPositive = getNewlyTestedPositive({
                newlyPositive,
                artStartDate,
                financialQuarterStart: financialQStart,
                financialQuarterEnd: financialQEnd,
                referralsDuringYear: referralsDuringQuarter,
                hivStatus,
            });

            const [facility, artNo, onArt] = getMultiAttributes(
                ["usRWNcogGX7", "aBc9Lr1z25H", "xyDBnQTdZqS"],
                viralLoadsB4Quarter
            );

            const OVC_TST_ASSESS = currentRiskAssessment ? 1 : 0;

            const {
                notAtRisk,
                notAtRiskAdult,
                tbScreeningChild,
                tbScreeningChild17,
                tbScreeningAdult,
                atTBRiskChild,
                atTBRiskChild17,
                atTBRiskAdult,
            } = getRiskAssessment(currentRiskAssessment);

            const atRiskOfTB =
                (atTBRiskChild >= 5 && age < 16) ||
                (atTBRiskAdult >= 5 && age > 17) ||
                (atTBRiskChild17 >= 5 && age >= 16)
                    ? 1
                    : 0;

            const isNotAtRisk = getIsNotAtRisk(
                hivStatus,
                notAtRiskAdult,
                notAtRisk
            );

            const serviceProvided = getDataElement(
                "XWudTD2LTUQ",
                currentReferral
            );
            const unknownOther = findAnyEventValue(
                riskAssessmentsDuringQuarter,
                "cTV8aMqnVbe"
            );
            const linked = deHasAnyValue(serviceProvided, [
                "Started HIV treatment",
                "PEP",
                "HCT/ Tested for HIV",
                "Intensive Adherence Counseling (IAC)",
                "Viral Load Testing",
                "Provided with ARVs",
            ]);

            riskFactor = hivStatus === "+" && age < 18 ? "CLHIV" : riskFactor;

            const testedForHIV =
                serviceProvided === "HCT/ Tested for HIV" ? 1 : 0;
            const primaryCareGiver = riskFactor === "Primary caregiver" ? 1 : 0;
            const OVC_TST_REFER =
                serviceProvided === "HCT/ Tested for HIV" ? 1 : 0;
            const OVC_TST_REPORT = hivResult && OVC_TST_REFER === 1 ? 1 : 0;
            const {
                memberStatus,
                householdStatus,
                clientMemberStatus: clientMemberStatus2,
            } = findStatus(
                homeVisitsB4Quarter,
                currentHomeVisit,
                hasEnrollment
            );

            const enrolledInSchool = isAtSchool(
                age,
                currentHomeVisit?.["OsOZF4e4yh5"],
                h4pXErY01YR
            );

            const homeVisitor = findAnyEventValue(
                homeVisitsB4Quarter,
                "i6XGAmzx3Ri"
            );

            const dataEntrant1 = Xkwy5P2JG24;

            const dataEntrant2 = findAnyEventValue(
                viralLoadDuringQuarter,
                "YY5zG4Bh898"
            );

            const dataEntrant =
                findAnyEventValue(homeVisitsDuringQuarter, "YY5zG4Bh898") ||
                dataEntrant1 ||
                dataEntrant2;

            const homeVisitorContact = findAnyEventValue(
                homeVisitsB4Quarter,
                "BMzryoryhtX"
            );
            const newlyEnrolledText = newlyEnrolled ? "Yes" : "No";

            const {
                VLTestDone,
                ovcEligible,
                ovcVL,
                VLStatus,
                VLSuppressed,
                copies,
            } = hivInformation({
                artStartDate,
                hivStatus,
                quarterEnd,
                lastViralLoadDate,
                viralTestDone,
                viralLoadResultsReceived,
                viralLoadCopies,
                viralLoadStatus,
            });

            const bankLinkages = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["xUW1Gx6g7mn", "VOkal1836K0"]
            );
            const agricLinkages = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["BjjU0DuSJRJ"]
            );
            const dreams = eventsHasDataElements(serviceLinkagesDuringQuarter, [
                "sJY2dId52Pv",
            ]);
            const vmmc = eventsHasDataElements(serviceLinkagesDuringQuarter, [
                "LGGiilKOYvf",
            ]);

            const micro = eventsHasDataElements(serviceLinkagesDuringQuarter, [
                "QzzTM8u8USa",
                "hzuzLSgcOsL",
            ]);

            const igaBooster = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["Dk5MrVc8DCO"]
            );
            const vocationalApprenticeship = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["E7WqYprrglQ"]
            );
            const governmentSocialProtection = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["fyZzO01L4oO"]
            );
            const outputMarkets = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["pJTLmJDJDyA"]
            );
            const supportedToEnroll = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["zugisWwepeI"]
            );

            const igaRegisteringSuccess = eventsHasDataElements(
                incomeGeneratingActivitiesB4Quarter,
                ["fyZzO01L4oO"]
            );

            const tempConsumption =
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "HBOascaLodU",
                ]) ||
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Temporary Food Support"]
                );

            const vlsaOvcFund = anyEventWithAnyOfTheValue(
                serviceLinkagesDuringQuarter,
                "NxQ4EZUB0fr",
                ["UF3 VSLA OVC protection Fund"]
            );
            const educationFund = anyEventWithAnyOfTheValue(
                serviceLinkagesDuringQuarter,
                "NxQ4EZUB0fr",
                ["UF09 OVC VSLA Education Fund"]
            );
            const educationSubsidy =
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "N6vri2eGvIr",
                    "Np1E6nDdbNQ",
                    "cKdecGMnNnz",
                    "hn3a5FQCtkT",
                    "OoBgArxswBi",
                    "x6qjZOXRElL",
                ]) ||
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Educational support"]
                );
            const nonFormalEducation =
                anyEventWithAnyOfTheValue(
                    serviceLinkagesDuringQuarter,
                    "NxQ4EZUB0fr",
                    ["O2. None Formal Education"]
                ) ||
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Vocational/Apprenticeship"]
                );
            const homeLearning = anyEventWithAnyOfTheValue(
                serviceLinkagesDuringQuarter,
                "NxQ4EZUB0fr",
                ["Home Learning"]
            );
            const healthFund = anyEventWithAnyOfTheValue(
                serviceLinkagesDuringQuarter,
                "NxQ4EZUB0fr",
                ["UF10 OVC VSLA Health Fund"]
            );

            const educationInformation =
                eventsHasDataElements(
                    homeVisitsDuringQuarter,
                    getSectionDataElements("d6m2LRGqJnn")
                ) === 1 && age >= 6
                    ? 1
                    : 0;

            const HTSReferral =
                deHasAnyValue(serviceProvided, [
                    "Started HIV treatment",
                    "PEP",
                    "HCT/ Tested for HIV",
                    "Intensive Adherence Counseling (IAC)",
                    "Viral Load Testing",
                    "Provided with ARVs",
                ]) ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "qGRGyK6uRaI",
                ]);

            const nonDisclosureSupport = eventsHasDataElements(
                homeVisitsDuringQuarter,
                getSectionDataElements("ce3AQYScARV")
            );
            const artInitiation = anyEventWithAnyOfTheValue(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                ["Initiated on HIV Treatment"]
            );

            const attachedToCorps = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["mCxdn8HSVbn"]
            );

            const homeDrugDelivery = deHasAnyValue(serviceProvided, [
                "Home drug delivery",
            ]);

            const artAdherenceEducation = eventsHasDataElements(
                homeVisitsDuringQuarter,
                getSectionDataElements("ZfSNO5akutD")
            );
            const hivCareAndLiteracy = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["F6dQ1A72dZD"]
            );

            const iac =
                anyEventWithDataElement(
                    viralLoadDuringQuarter,
                    "iHdNYfm1qlz",
                    "true"
                ) ||
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Intensive Adherence Counseling (IAC)"]
                )
                    ? 1
                    : 0;
            const eMTCT = eventsHasDataElements(
                homeVisitsDuringQuarter,
                getSectionDataElements("hwzwC18yXkZ")
            );

            const hivPrevention = eventsHasDataElements(
                homeVisitsDuringQuarter,
                getSectionDataElements("UefB1vs1yM0")
            );

            const TFHealth =
                anyEventWithAnyOfTheValue(
                    serviceLinkagesDuringQuarter,
                    "NxQ4EZUB0fr",
                    ["Transport to Facility"]
                ) ||
                eventsHasDataElements(homeVisitsDuringQuarter, [
                    "EG851ch1rWZ",
                    "gLj768y0v9Y",
                    "xdxz9xJ7USt",
                ]);

            const PEP = anyEventWithAnyOfTheValue(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                ["PEP"]
            )
                ? 1
                : 0;

            const covid19Education = eventsHasDataElements(
                homeVisitsDuringQuarter,
                getSectionDataElements("pRmqIxwwWiz")
            );

            const immunization = anyEventWithAnyOfTheValue(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                ["Immunisation"]
            );

            const immunisationStatus = getAttribute(
                "RMl0p06BQ9j",
                currentHomeVisit
            );

            const wash =
                anyEventWithDE(homeVisitsDuringQuarter, "eEZu3v92pJZ") ||
                eventsHasDataElements(
                    homeVisitsDuringQuarter,
                    getSectionDataElements("kRLvRElkBMc")
                ) === 1
                    ? 1
                    : 0;

            const treatedNets = eventsHasDataElements(homeVisitsDuringQuarter, [
                "Cnjs7y3Rvi0",
            ]);

            const familyPlanning = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["q2Pop0z4hrt", "oJfyYYOgbqs", "wUGLrnw0SV6"]
            );
            const initiatedOnTB = anyEventWithAnyOfTheValue(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                ["Initiated on TB Treatment"]
            );
            const tested4TB = anyEventWithAnyOfTheValue(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                ["Tested for TB"]
            );

            const supported2CompleteTBDose = anyEventWithAnyOfTheValue(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                ["Supported to Complete TB Dose"]
            );

            const viralLoadBleeding =
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Viral Load Testing"]
                ) === 1 ||
                anyEventWithAnyOfTheValue(
                    serviceLinkagesDuringQuarter,
                    "NxQ4EZUB0fr",
                    ["HTS7. Viral load test"]
                ) === 1;

            const returnedToCare = anyEventWithAnyOfTheValue(
                serviceLinkagesDuringQuarter,
                "NxQ4EZUB0fr",
                ["PLHIV Returned to care"]
            );

            const otherHealthServices =
                anyEventWithDE(homeVisitsDuringQuarter, "eEZu3v92pJZ") ||
                anyEventWithDE(homeVisitsDuringQuarter, "D7rrGXWwjGn") ||
                anyEventWithDE(homeVisitsDuringQuarter, "CnfRJ2y4Lg8")
                    ? 1
                    : 0;

            const tbScreening =
                (tbScreeningChild === 4 && age < 16) ||
                (tbScreeningAdult === 4 && age > 17) ||
                (tbScreeningChild17 === 4 && age >= 16)
                    ? 1
                    : 0;

            const unknown = getUnknownStatus({
                hivStatus,
                riskFactor,
                notAtRisk,
                isNotAtRisk,
                age,
                unknownOther,
            });

            const newlyTestedAndOnArt = getNewlyTestedAndOnArt({
                newlyTestedPositive,
                artStartDate,
                onArt,
                serviceProvided,
                financialQuarterStart: financialQStart,
                financialQuarterEnd: financialQEnd,
            });

            const clientMemberStatus = currentViralLoad?.["tkyfofbEzEc"] ?? "";
            const sampleType = currentViralLoad?.["RmhO4qcsC2Z"] ?? "";

            const onMultiMonthDispensing = convertBoolToYesNo(
                currentViralLoad?.["XZzjyuqPs0p"]
            );

            const clientDSDModel = currentViralLoad?.["RvvlK3akoaQ"] ?? "";
            const currentTBStatus = currentViralLoad?.["c9huL0msMQ7"] ?? "";
            const onTBTreatment = currentViralLoad?.["T6Id5L85PDM"] ?? "";
            const hasThePersonDisclosed =
                currentViralLoad?.["iFgXXIUj9C0"] ?? "";
            const heiUptoDateWithImmunization =
                currentViralLoad?.["qkpSMaBL0eQ"] ?? "";

            const viralLoadIs12Months = monthsSinceViralTest(
                quarterEnd,
                12,
                lastViralLoadDate
            );
            const viralLoadIs6Months = monthsSinceViralTest(
                quarterEnd,
                6,
                lastViralLoadDate
            );

            const {
                missedAppointmentDate,
                missedAnAppointment,
                missedAnAppointmentReason,
                missedAnAppointmentFollowupOutcome,
                hasEverMissedAnAppointment,
                missedAnAppointmentAction,
            } = missedAppointmentInfo(missedAppointments, quarterEnd);
            const VSLA = directBeneficiariesB4Quarter.length > 0 ? 1 : 0;

            const directBeneficiariesOperatingIGA =
                incomeGeneratingActivitiesB4Quarter.length > 0 ? 1 : 0;

            const coreES = anyService([
                VSLA,
                bankLinkages,
                agricLinkages,
                tempConsumption,
                igaBooster,
                igaRegisteringSuccess,
                micro,
                vlsaOvcFund,
                VSLABorrowing,
                VSLASavings,
                vocationalApprenticeship,
                governmentSocialProtection,
                directBeneficiariesOperatingIGA,
                igaRegisteringSuccess,
                outputMarkets,
            ]);
            const currentSchool = getAttribute(
                "EYTmVQPfoh4",
                currentSchoolMapping
            );
            const currentClass = getAttribute(
                "pimaAP2qYYE",
                currentSchoolMapping
            );

            const monitoringAtSchool =
                schoolMonitoringB4Quarter.length > 0 ? 1 : 0;
            const hasGbvScreening =
                GBVScreeningDuringQuarter.length > 0 ? 1 : 0;

            const coreEducation = anyService([
                educationFund,
                educationSubsidy,
                homeLearning,
                educationInformation,
                nonFormalEducation,
            ]);

            const communityViralLoadBleeding = deHasAnyValue(serviceProvided, [
                "Viral Load Testing",
            ]);

            const coreHealth = anyService([
                HTSReferral,
                nonDisclosureSupport,
                artInitiation,
                artAdherenceEducation,
                iac,
                eMTCT,
                hivPrevention,
                // journeysMOH,
                // journeysLARA,
                // NMNBoys,
                // NMNGirls,
                TFHealth,
                PEP,
                covid19Education,
                otherHealthServices,
                homeDrugDelivery,
                tested4TB,
                initiatedOnTB,
                wash,
                treatedNets,
                familyPlanning,
                healthFund,
                TFHealth,
                supported2CompleteTBDose,
                immunization,
            ]);

            const emotional1 = anyEventWithDataElement(
                GBVScreeningDuringQuarter,
                "IHcLv90cUNq",
                "true"
            );
            const emotional2 = anyEventWithDataElement(
                GBVScreeningDuringQuarter,
                "diWuTE7rxUk",
                "true"
            );
            const physicalAbuse = anyEventWithDataElement(
                GBVScreeningDuringQuarter,
                "chX1ZE4MQuB",
                "true"
            );

            const sexual1 = anyEventWithDataElement(
                GBVScreeningDuringQuarter,
                "HZd8eEGyZc4",
                "true"
            );
            const sexual2 = anyEventWithDataElement(
                GBVScreeningDuringQuarter,
                "UMHo3JZKT5Y",
                "true"
            );
            const sexualAbuse = sexual1 !== undefined || sexual2 !== undefined;

            const emotionalAbuse =
                emotional1 !== undefined || emotional2 !== undefined;

            const reportedGBV =
                emotional1 !== undefined ||
                emotional2 !== undefined ||
                physicalAbuse !== undefined ||
                sexual1 !== undefined ||
                sexual2 !== undefined;

            const GBVCounseling = eventsHasDataElements(
                GBVScreeningDuringQuarter,
                ["MPQPmunSbKm"]
            );
            const GBVReferral = eventsHasDataElements(
                GBVScreeningDuringQuarter,
                ["CVHBWfo9zcw"]
            );

            const IPVHIVDisclosure = eventsHasDataElements(
                homeVisitsDuringQuarter,
                [
                    "d8d52oyjouy",
                    "fU9JasieC5b",
                    "hFjerhDyBKS",
                    "XCvSBlgksT0",
                    "QAmU3eQcj4j",
                    "ycYzqNr0vl5",
                    "NT5y7WQQcvG",
                ]
            );

            const TFGBV =
                anyEventWithDataElement(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    "Transport GBV"
                ) ||
                anyEventWithDataElement(
                    serviceLinkagesDuringQuarter,
                    "NxQ4EZUB0fr",
                    "Transport GBV"
                )
                    ? 1
                    : 0;

            const withdrawnFromGVBHousehold = eventsHasDataElements(
                homeVisitsDuringQuarter,
                [
                    "XOnEhSTi1YZ",
                    "tuW0QehIxbD",
                    "labqQpjQIVS",
                    "UXoUP6m5x3n",
                    "hFjerhDyBKS",
                    "eEm981b9iAT",
                    "SlVjFqZKz3U",
                    "sjmGNT2QGYU",
                    "ruSwtO1J9CV",
                ]
            );
            const GBVLegalSupport = eventsHasDataElements(
                homeVisitsDuringQuarter,
                [
                    "KmxUqMzSSWT",
                    "dvQHNVbEGPb",
                    "srAJAZLBf9h",
                    "SiwjAG5Z7n1",
                    "d8d52oyjouy",
                    "leOn4N7Irta",
                    "c8SFOQxWyOm",
                    "vRxHFjnbnwf",
                    "zDvcGqjmSQr",
                ]
            );
            const basicNeed = eventsHasDataElements(homeVisitsDuringQuarter, [
                "ORCzvst8msI",
                "hYY3ot7ZaKl",
                "GkZv3TbBPnn",
                "NIelSVuSrvQ",
                "Pv5x5K8nysq",
                "XCvSBlgksT0",
                "fyQaqkQIXAq",
                "RSYM3WHKVqT",
                "eGfIisC9M9g",
                "dMvIT2yhzIR",
            ]);
            const legalSupport = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["rMYeO3sp2en", "zlDhryIk7OU"]
            );
            const reIntegration = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["Ef9jKdJk9No"]
            );
            const withdrawFromLabour = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["siC6lGt6qOs", "eKsXun8eAfV"]
            );

            const handleChildAbuse = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["AfkLGLnEft4", "y8Pq26t7CyW", "Rjyb7p8aWMx"]
            );
            const birthRegistration = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["A30d8MXwzhX", "sdoMZC7cD9S"]
            );

            const childProtectionEducation = eventsHasDataElements(
                homeVisitsDuringQuarter,
                [
                    "vQeVaiEJfmM",
                    "rsywcqeAWeD",
                    "ZekYkAu0olk",
                    "OhqAmjjqJNc",
                    "xRxFcns3aew",
                    "FTvLP1jSnqT",
                    "iIQMfRchN5q",
                    "zgYnmrUlwvb",
                    "gT67vyCjAlS",
                    "mQFKcNwfJlW",
                ]
            );
            const coreChildProtection = anyService([
                basicNeed,
                legalSupport,
                reIntegration,
                withdrawFromLabour,
                birthRegistration,
                childProtectionEducation,
            ]);

            const nutritionEducation = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["mSruDFpElU1"]
            );

            const nutritionalStatus = findAnyEventValue(
                homeVisitsDuringQuarter,
                "a3x9L2p3N6u"
            );
            const mentalHealth = findAnyEventValue(
                homeVisitsDuringQuarter,
                "sFgpQOWdusS"
            );
            const nutritionalAssessment = nutritionalStatus ? 1 : 0;
            const voucher4Crops =
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "vFxB1KqM5lp",
                ]) === 1 ||
                eventsHasDataElements(homeVisitsDuringQuarter, [
                    "sPzsTkUzj73",
                ]) === 1;
            const kitchenGarden = anyEventHasDataElementValue(
                serviceLinkagesDuringQuarter,
                "lucS67EnDeo",
                "true"
            );
            const nutritionalFoodSupplement =
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "HBOascaLodU",
                ]) ||
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Temporary Food Support"]
                );

            const farmingInputs = eventsHasDataElements(
                homeVisitsB4Quarter,
                getSectionDataElements("sIyiDhFjOqe")
            );

            const agricAdvisoryService = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["uSFFWSRwLfz", "R9sLOPh4rvo", "Apq9oZy3tN2", "PWObO6CYTOA"]
            );

            const coreNutrition = anyService([
                nutritionEducation,
                kitchenGarden,
                nutritionalAssessment,
                kitchenGarden,
                nutritionalFoodSupplement,
            ]);

            const recreationActivities = eventsHasDataElements(
                homeVisitsDuringQuarter,
                [
                    "GR5cDUAEvxK",
                    "Rd2RkCviwMb",
                    "obkZ4AVkPf3",
                    "MpmKR1jTGPa",
                    "ycYzqNr0vl5",
                    "WHZO2MYCVuA",
                    "WP3Jdhog1tQ",
                    "QCuWnztP0pi",
                    "Az2vyIEXkmE",
                ]
            );
            const willWriting = eventsHasDataElements(homeVisitsDuringQuarter, [
                "as9t4IWFo18",
            ]);
            const assistiveDevices = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["ctxofPwv89O", "FnYsJk15LW0"]
            );
            const { fullyGraduated, preGraduated } = getGraduationInfo(
                mostRecentGraduation,
                quarterEnd
            );

            const quarter = anyService([
                coreES,
                coreEducation,
                coreHealth,
                coreChildProtection,
                coreNutrition,
            ]);

            const psychosocialSupport = quarter;

            const corePSS = psychosocialSupport === 1 ? 1 : 0;

            allPreviousLayering = {
                ...allPreviousLayering,
                [qtr]: { quarter, fullyGraduated },
            };
            const servedInPreviousQuarter = allPreviousLayering[previousQuarter]
                ? allPreviousLayering[previousQuarter]["quarter"]
                : 0;
            const { OVC_ENROL, OVC_HIV_STAT, OVC_SERV, OVC_SERV_SUBPOP } =
                getOVCInfo({
                    newlyEnrolled,
                    quarter,
                    notAtRisk,
                    notAtRiskAdult,
                    age,
                    ovcVL,
                    servedInPreviousQuarter,
                    hivStatus,
                    risks,
                    riskFactor,
                });

            let On_ART_HVAT: string = "";

            if (hivStatus !== "+" && umqeJCVp4Zq === "NA") {
                On_ART_HVAT = "";
            } else if (hivStatus === "+") {
                On_ART_HVAT = umqeJCVp4Zq === "Yes" ? "1" : "0";
            }

            const assetOwnership = findAssetOwnership(filtered, quarterEnd);
            const exitedWithGraduation = getGraduationStatus({
                memberStatus,
                quarter,
                OVC_SERV,
                servedInPreviousQuarter,
                newlyEnrolled,
            });

            layering.push({
                ...currentLayer,
                primaryCareGiver,
                baselineRiskFactor,
                otherRiskFactor,
                reasonForExit,
                isNotAtRisk,
                unknown,
                newlyTestedPositive,
                newlyTestedAndOnArt,
                clientMemberStatus,
                viralLoadIs12Months,
                viralLoadIs6Months,
                sampleType,
                onMultiMonthDispensing,
                clientDSDModel,
                currentTBStatus,
                onTBTreatment,
                hasThePersonDisclosed: convertBoolToYesNo(
                    hasThePersonDisclosed
                ),
                heiUptoDateWithImmunization,
                bankLinkages,
                agricLinkages,
                vocationalApprenticeship,
                governmentSocialProtection,
                homeVisitor,
                currentSchool,
                currentClass,
                monitoringAtSchool,
                supportedToEnroll,
                dreams,
                vmmc,
                voucher4Crops,
                hasGbvScreening,
                reportedGBV,
                GBVCounseling,
                GBVReferral,
                attachedToCorps,
                hivCareAndLiteracy,
                communityViralLoadBleeding,
                qtr,
                id,
                age,
                ageGroup,
                dob,
                economicBaseline: getEconomicStatus(firstHVAT),
                economicStatus: getEconomicStatus(currentHVAT),
                eventDate,
                facility,
                artNo,
                onArt: convertBoolToNum(onArt),
                weight,
                artStartDate,
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
                pcr,
                newlyPositive,
                newlyEnrolledText,
                riskFactor,
                householdStatus,
                memberStatus,
                enrolledInSchool,
                newlyEnrolled,
                hivStatus,
                OVC_TST_ASSESS,
                OVC_TST_REFER,
                OVC_TST_REPORT,
                linked,
                testedForHIV,
                n7VQaJ8biOJ,
                lastViralLoadDate,
                currentRegimen: regimen,
                ovcEligible,
                VLTestDone,
                ovcVL,
                VLStatus,
                copies,
                VLSuppressed,
                immunisationStatus,
                VSLA,
                directBeneficiariesOperatingIGA,
                igaRegisteringSuccess,
                VSLASavings,
                VSLABorrowing,
                outputMarkets,
                fLiteracy: "",
                agriBusiness: "",
                spmTraining: "",
                micro,
                igaBooster,
                tempConsumption,
                vlsaOvcFund,
                coreES,
                educationSubsidy,
                homeLearning,
                nonFormalEducation,
                educationInformation,
                educationFund,
                coreEducation,
                healthFund,
                HTSReferral,
                nonDisclosureSupport,
                artInitiation,
                homeDrugDelivery,
                artAdherenceEducation,
                viralLoadBleeding,
                returnedToCare,
                iac,
                eMTCT,
                hivPrevention,
                journeysMOH: "",
                journeysLARA: "",
                NMNBoys: "",
                NMNGirls: "",
                TFHealth,
                PEP,
                covid19Education,
                immunization,
                wash,
                treatedNets,
                familyPlanning,
                tbScreening,
                atRiskOfTB,
                tested4TB,
                initiatedOnTB,
                supported2CompleteTBDose,
                otherHealthServices,
                coreHealth,
                TFGBV,
                ECD: "",
                parentingAttended: "",
                parenting: "",
                nutritionEducation,
                farmingInputs,
                nutritionalAssessment,
                nutritionalStatus,
                kitchenGarden: convertBoolToYesNo(kitchenGarden),
                nutritionalFoodSupplement,
                coreNutrition,
                psychosocialSupport,
                mentalHealth,
                recreationActivities,
                assistiveDevices,
                corePSS,
                preGraduated,
                fullyGraduated,
                servedInCurrentQuarter: quarter,
                servedInPreviousQuarter,
                graduated: "",
                OVC_SERV,
                OVC_ENROL,
                OVC_SERV_SUBPOP,
                OVC_HIV_STAT,
                exitedWithGraduation,
                otherPERFARIP: "",
                otherIP: "",
                On_ART_HVAT,
                homeVisitorContact,
                dataEntrant,
                assetOwnership,
                deleted,
                inactive,
                missedAppointmentDate,
                missedAnAppointment,
                missedAnAppointmentReason,
                missedAnAppointmentFollowupOutcome,
                hasEverMissedAnAppointment,
                missedAnAppointmentAction,
                linkedToDSDM:
                    missedAnAppointmentAction === "3d2. Linked to DSDM (MMD)"
                        ? 1
                        : 0,
                appointmentReminding:
                    missedAnAppointmentAction === "3d5. Appointment reminding"
                        ? 1
                        : 0,
                orgUnit,
                regimen,
                agricAdvisoryService,
                emotionalAbuse,
                sexualAbuse,
                physicalAbuse,
                IPVHIVDisclosure,
                withdrawnFromGVBHousehold,
                GBVLegalSupport,
                basicNeed,
                legalSupport,
                reIntegration,
                withdrawFromLabour,
                handleChildAbuse,
                birthRegistration,
                childProtectionEducation,
                coreChildProtection,
                willWriting,
                NMNInstructor: homeVisitor,
                paraSocialWorker: homeVisitorContact,
                householdExitReason,
                clientMemberStatus2,
                reasonForVisit,
            });
        }
    }
    return layering;
};

const worker = new Worker<QueryDslQueryContainer>(
    "query",
    async (job) => {
        await scroll3("RDEklSXCD4C", job.data, async (documents) => {
            const allData = await fetchData(documents);
            const layering = generateLayering({
                ...allData,
                periods: [
                    // dayjs().subtract(12, "quarters"),
                    // dayjs().subtract(11, "quarters"),
                    // dayjs().subtract(10, "quarters"),
                    // dayjs().subtract(9, "quarters"),
                    // dayjs().subtract(8, "quarters"),
                    // dayjs().subtract(7, "quarters"),
                    // dayjs().subtract(6, "quarters"),
                    // dayjs().subtract(5, "quarters"),
                    dayjs().subtract(4, "quarters"),
                    dayjs().subtract(3, "quarters"),
                    dayjs().subtract(2, "quarters"),
                    dayjs().subtract(1, "quarters"),
                    dayjs(),
                ],
                trackedEntityInstances: documents,
            });

            await indexBulk("layering", layering);
        });
    },
    { connection }
);

worker.on("completed", (job) => {
    console.log(`${job.id} has completed!`);
});

worker.on("failed", (job, err) => {
    console.log(`${job?.id} has failed with ${err.message}`);
});
