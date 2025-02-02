import { QueryDslQueryContainer } from "@elastic/elasticsearch/lib/api/types";
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
    convertSearchTo10,
    convertViralStatus,
    deHasAnyValue,
    eventsBeforePeriod,
    eventsHasDataElements,
    eventsHasDataElementsWithValue,
    eventsWithinPeriod,
    fetchGroupActivities4Instances,
    findAgeGroup,
    findAnyEventValue,
    findAssetOwnership,
    findStatus,
    getAttribute,
    getAttributes,
    getDataElement,
    getEconomicStatus,
    getGraduationAssessment,
    getGraduationStatus,
    getHEIInformation,
    getHIVStatus,
    getIsNotAtRisk,
    getNewlyPositive,
    getNewlyTestedAndOnArt,
    getNewlyTestedPositive,
    getOVCInfo,
    getRiskAssessment,
    getSectionDataElements,
    getUnknownStatus,
    hivInformation,
    latestEvent,
    missedAppointmentInfo,
    monthsSinceViralTest,
    removeEmptyKeys,
    scroll,
    scroll3,
    scroll4
} from "./utils";
import { logger } from "./logger";

dayjs.extend(isoWeek);
dayjs.extend(quarterOfYear);
dayjs.extend(advancedFormat);

export const layeringQueue = new Queue<QueryDslQueryContainer | any[]>("query", {
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
                ]),
            ),
        ]),
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
    trackedEntityInstance: string,
) => {
    return available[trackedEntityInstance] || [];
};

const fetchData = async (trackedEntityInstances: any[]) => {
    const trackedEntityInstanceIds = trackedEntityInstances.map(
        (tei) => tei.trackedEntityInstance,
    );
    const allInstances = uniq(
        trackedEntityInstances.map(({ hly709n51z0 }) => hly709n51z0),
    ).filter((v) => !!v);
    const previousLayer = await previousLayering(trackedEntityInstanceIds);
    const allHomeVisits = await scroll("HaaSLv2ur0l", trackedEntityInstanceIds);
    const allHivRiskAssessments = await scroll(
        "B9EI27lmQrZ",
        trackedEntityInstanceIds,
    );
    const allViralLoads = await scroll("kKlAyGUnCML", trackedEntityInstanceIds);
    const allReferrals = await scroll("yz3zh5IFEZm", trackedEntityInstanceIds);
    const allServiceLinkages = await scroll(
        "SxnXrDtSJZp",
        trackedEntityInstanceIds,
    );
    const indexCases = await scroll4("HEWq6yr4cs5", allInstances);
    const allHVATAssessments = await scroll("sYE3K7fFM4Y", allInstances); // use indexCases instead of allInstances
    const allGraduationAssessments = await scroll(
        "Cx35Elpu330",
        allInstances,
        ["trackedEntityInstance,eventDate,XPJtNCSNCdR"],
    );
    const allIndividualGraduationAssessments = await scroll(
        "CS61IdHynTk",
        trackedEntityInstanceIds,
    );
    const allMissedAppointments = await scroll(
        "qNxRoC1wIYA",
        trackedEntityInstanceIds,
    );
    const allDirectBeneficiaries = await scroll(
        "Hg8f2oHcKvH",
        trackedEntityInstanceIds,
    );
    const allGroupActivities = await fetchGroupActivities4Instances(
        trackedEntityInstances,
    );
    const allProtectionFunds = await scroll(
        "pyT5GQSyHZG",
        trackedEntityInstanceIds,
    );
    const allIncomeGeneratingActivities = await scroll(
        "yF0ujXkemkw",
        trackedEntityInstanceIds,
    );
    const allSchoolMappings = await scroll(
        "zAaPkLNSWwL",
        trackedEntityInstanceIds,
    );
    const allSchoolMonitoring = await scroll(
        "NwazREY6KZM",
        trackedEntityInstanceIds,
    );
    const allGBVScreening = await scroll(
        "SGChaH7CoMA",
        trackedEntityInstanceIds,
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
        allIndividualGraduationAssessments,
    };
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
    allIndividualGraduationAssessments: { [key: string]: any[] };
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
        allIndividualGraduationAssessments,
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
        l4jlzkYsUoR,
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
        console.log("Home visits:",homeVisits)
        const graduationAssessments = getEvents(
            allIndividualGraduationAssessments,
            trackedEntityInstance,
        );
        const hivRiskAssessments = getEvents(
            allHivRiskAssessments,
            trackedEntityInstance,
        );
        const viralLoads = getEvents(allViralLoads, trackedEntityInstance);
        const referrals = getEvents(allReferrals, trackedEntityInstance);
        const serviceLinkages = getEvents(
            allServiceLinkages,
            trackedEntityInstance,
        );
        const missedAppointments = getEvents(
            allMissedAppointments,
            trackedEntityInstance,
        );
        const directBeneficiaries = getEvents(
            allDirectBeneficiaries,
            trackedEntityInstance,
        );
        const protectionFunds = getEvents(
            allProtectionFunds,
            trackedEntityInstance,
        );
        const incomeGeneratingActivities = getEvents(
            allIncomeGeneratingActivities,
            trackedEntityInstance,
        );
        const schoolMappings = getEvents(
            allSchoolMappings,
            trackedEntityInstance,
        );
        const schoolMonitoring = getEvents(
            allSchoolMonitoring,
            trackedEntityInstance,
        );
        const GBVScreenings = getEvents(allGBVScreening, trackedEntityInstance);
        const hasEnrollment = !!enrollmentDate;

        const HVATAssessments = allHVATAssessments[hly709n51z0] || [];

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
        let currentLayer: Record<string, any> = {
            trackedEntityInstance,
            beneficiaryId: HLKc2AKR9jW,
            e0zEpTw7IH6,
            householdCode: tHCT4RKXoiU, // Household code
            enrollmentDate,
            type: l4jlzkYsUoR,
            district,
            subCounty,
            parish: orgUnitName,
            village: Xkwy5P2JG24,
            IyKRQFkfwMk, // name of service provider
            householdHead: ExnzeYjgIaT,
            beneficiaryName: huFucxA3e5c,
            /// N1nMqKtYKvI, // see dob
            sex: CfpoFtRmK1z,
            umqeJCVp4Zq, // see on_HIV_ART
            /// householdCode: r10igcWrpoH, // Household code check
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
            ["desc"],
        );

        for (const period of periods) {
            const quarterStart = period.startOf("quarter");
            const quarterEnd = period.endOf("quarter");

            const [financialQuarterStart, financialQuarterEnd] =
                calculateQuarter(quarterStart.year(), period.quarter());
            const qtr = period.format("YYYY[Q]Q");
            const id = `${trackedEntityInstance}${qtr}`;
            const age = period.diff(dayjs(dob), "years");
            const ageGroup = findAgeGroup(age);
            const previousQuarter = quarterStart
                .subtract(1, "quarters")
                .format("YYYY[Q]Q");
            const viralLoadsB4Quarter = eventsBeforePeriod(
                viralLoads,
                quarterEnd,
            );
            const homeVisitsB4Quarter = eventsBeforePeriod(
                homeVisits,
                quarterEnd,
            );

            // const directBeneficiariesB4Quarter = eventsBeforePeriod(
            //     directBeneficiaries,
            //     quarterEnd,
            // );

            const graduationAssessmentsB4Quarter = eventsBeforePeriod(
                graduationAssessments,
                quarterEnd,
            );

            const incomeGeneratingActivitiesB4Quarter = eventsBeforePeriod(
                incomeGeneratingActivities,
                quarterEnd,
            );
            const incomeGeneratingActivitiesDuringQuarter = eventsWithinPeriod(
                incomeGeneratingActivities,
                quarterStart,
                quarterEnd,
            );
            // const schoolMappingsB4Quarter = eventsBeforePeriod(
            //     schoolMappings,
            //     quarterEnd,
            // );
            // const schoolMonitoringB4Quarter = eventsBeforePeriod(
            //     schoolMonitoring,
            //     quarterEnd,
            // );

            const riskAssessmentsDuringQuarter = eventsWithinPeriod(
                hivRiskAssessments,
                quarterStart,
                quarterEnd,
            );

            const referralsDuringQuarter = eventsWithinPeriod(
                referrals,
                quarterStart,
                quarterEnd,
            );
            const serviceLinkagesDuringQuarter = eventsWithinPeriod(
                serviceLinkages,
                quarterStart,
                quarterEnd,
            );
            const homeVisitsDuringQuarter = eventsWithinPeriod(
                homeVisits,
                quarterStart,
                quarterEnd,
            );
            const viralLoadDuringQuarter = eventsWithinPeriod(
                viralLoads,
                quarterStart,
                quarterEnd,
            );
            // const protectionFundsDuringQuarter = eventsWithinPeriod(
            //     protectionFunds,
            //     quarterStart,
            //     quarterEnd,
            // );
            const missedAppointmentsDuringQuarter = eventsWithinPeriod(
                missedAppointments,
                quarterStart,
                quarterEnd,
            );
            // const GBVScreeningDuringYear = eventsWithinPeriod(
            //     GBVScreenings,
            //     quarterStart,
            //     quarterEnd
            // );

            const directBeneficiariesDuringQuarter = eventsWithinPeriod(
                directBeneficiaries,
                quarterStart,
                quarterEnd,
            );
            // const directBeneficiariesDuringYear = eventsWithinPeriod(
            //     directBeneficiaries,
            //     financialQuarterStart,
            //     financialQuarterEnd,
            // );

            // const incomeGeneratingActivitiesDuringYear = eventsWithinPeriod(
            //     incomeGeneratingActivities,
            //     financialQuarterStart,
            //     financialQuarterEnd,
            // );
            const schoolMappingDuringYear = eventsWithinPeriod(
                incomeGeneratingActivities,
                financialQuarterStart,
                financialQuarterEnd,
            );

            const schoolMonitoringDuringYear = eventsWithinPeriod(
                schoolMonitoring,
                financialQuarterStart,
                financialQuarterEnd,
            );
            // TODO pick a question from GBV tool
            const GBVScreeningDuringYear = eventsWithinPeriod(
                GBVScreenings,
                financialQuarterStart,
                financialQuarterEnd,
            );

            const currentGBVScreening = latestEvent(GBVScreeningDuringYear);
            const currentViralLoad = latestEvent(viralLoadsB4Quarter);
            const currentGraduationAssessment = latestEvent(
                graduationAssessmentsB4Quarter,
            );
            const baselineViralLoad = baselineEvent(viralLoadsB4Quarter);
            const currentHomeVisit = latestEvent(homeVisitsDuringQuarter);
            const currentDirectBeneficiary = latestEvent(
                directBeneficiariesDuringQuarter,
            );
            const currentReferral = latestEvent(referralsDuringQuarter);
            const currentRiskAssessment = latestEvent(
                riskAssessmentsDuringQuarter,
            );
            const currentHVAT = latestEvent(HVATAssessments);
            const firstHVAT = baselineEvent(HVATAssessments);
            const firstHomeVisit = baselineEvent(homeVisitsB4Quarter);
            const currentSchoolMapping = latestEvent(schoolMappingDuringYear);

            const previousViralLoads = eventsBeforePeriod(
                viralLoads,
                quarterStart,
            );
            const previousReferrals = eventsBeforePeriod(
                referrals,
                quarterStart,
            );

            let eventDate = "";

            if (currentHVAT) {
                eventDate = currentHVAT.eventDate;
            }
            const newlyEnrolled = dayjs(enrollmentDate).isBetween(
                quarterStart,
                quarterEnd,
            );

            let riskFactor = currentHomeVisit?.["rQBaynepqjy"];
            const districtOfRelocation = currentHomeVisit?.["VVFcQiYRQQL"];
            const subCountyOfRelocation = currentHomeVisit?.["pmAiKj6U4Jx"];
            const otherRiskFactor = currentHomeVisit?.["V7oko4Tm3N8"];

            const baselineRiskFactor =
                nDUbdM2FjyP || firstHomeVisit?.["rQBaynepqjy"];
            // const reasonForExit = currentHomeVisit?.["yiKbqQvYunj"] ?? "";
            const reasonForVisit = currentHomeVisit?.["t7kVwlLVBns"] ?? "";
            // const householdExitReason = currentHomeVisit?.["Xy3kS6Jgd08"] ?? "";
            const VSLASavings = currentDirectBeneficiary?.["H5vsW6LYFhy"] ?? "";
            const VSLABorrowing =
                currentDirectBeneficiary?.["s4w6hTytt5h"] ?? "";

            const artStartDate = getAttribute("epmIBD8gh7G", baselineViralLoad);

            const [
                weight,
                lastViralLoadDate,
                viralTestDone,
                viralLoadResultsReceived,
                viralLoadStatus,
                sampleType,
                viralLoadCopies,
                regimen,
                clientMemberStatus,
                onMultiMonthDispensing,
                clientDSDModel,
                currentTBStatus,
                onTBTreatment,
                hasThePersonDisclosed,
                heiUptoDateWithImmunization,
                currentTBPreventionStatus,
                facility,
                artNo,
                onArt,
                screened4TB,
                tbScreeningStatus,
                motherBabyPair,
                heiCode,
            ] = getAttributes(
                [
                    "fIbdjZvdOHt",
                    "Ti0huZXbAM0",
                    "cM7dovIX2Dl",
                    "te2VwealaBT",
                    "hY3VAB2NyRu",
                    "RmhO4qcsC2Z",
                    "b8p0uWaYRhY",
                    "nZ1omFVYFkT",
                    "tkyfofbEzEc",
                    "XZzjyuqPs0p",
                    "RvvlK3akoaQ",
                    "c9huL0msMQ7",
                    "T6Id5L85PDM",
                    "iFgXXIUj9C0",
                    "qkpSMaBL0eQ",
                    "nLwPogZRhau",
                    "usRWNcogGX7",
                    "aBc9Lr1z25H",
                    "xyDBnQTdZqS",
                    "MvrCiEIk5Ef",
                    "bBILr4VHj7B",
                    "ggCxBusYWSf",
                    "udCDAJdVI3H",
                ],
                currentViralLoad,
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

            const newlyReportedPositive = getNewlyPositive({
                newlyEnrolled,
                hivStatus,
                HzUL8LTDPga,
                previousReferrals,
                previousViralLoads,
            });

            const newlyTestedPositive = getNewlyTestedPositive({
                newlyReportedPositive,
                artStartDate,
                quarterStart,
                quarterEnd,
                referralsDuringYear: referralsDuringQuarter,
                hivStatus,
            });
            // TODO add vsla

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
                notAtRisk,
            );

            const serviceProvided = getDataElement(
                "XWudTD2LTUQ",
                currentReferral,
            );
            const unknownOther = findAnyEventValue(
                riskAssessmentsDuringQuarter,
                "cTV8aMqnVbe",
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
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["HCT/ Tested for HIV"],
                ) ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "LwcqnNevUcG",
                ]);
            const primaryCareGiver = riskFactor === "Primary caregiver" ? 1 : 0;
            const OVC_TST_REFER = testedForHIV;
            const OVC_TST_REPORT = hivResult && OVC_TST_REFER === 1 ? 1 : 0;
            const {
                memberStatus,
                householdStatus,
                householdExitReason,
                reasonForExit,
            } = findStatus(currentHomeVisit);

            const enrolledInSchool = convertBoolToYesNo(
                currentHomeVisit?.["OsOZF4e4yh5"],
            );

            const homeVisitor = findAnyEventValue(
                homeVisitsB4Quarter,
                "i6XGAmzx3Ri",
            );

            const dataEntrant1 = Xkwy5P2JG24;

            const dataEntrant2 = findAnyEventValue(
                viralLoadDuringQuarter,
                "YY5zG4Bh898",
            );

            const dataEntrant =
                findAnyEventValue(homeVisitsDuringQuarter, "YY5zG4Bh898") ||
                dataEntrant1 ||
                dataEntrant2;

            const homeVisitorContact = findAnyEventValue(
                homeVisitsB4Quarter,
                "BMzryoryhtX",
            );
            const newlyEnrolledText = newlyEnrolled ? "Yes" : "No";

            const { VLTestDone, ovcEligible, ovcVL, VLStatus, copies } =
                hivInformation({
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
                ["xUW1Gx6g7mn", "VOkal1836K0"],
            );
            const apprenticeship =
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "E7WqYprrglQ",
                ]) ||
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Vocational/Apprenticeship"],
                );
            const agricLinkages = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["BjjU0DuSJRJ"],
            );
            const microFranchise = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["aS0PFCSdpiw", "QzzTM8u8USa", "hzuzLSgcOsL"],
            );
            const dreams = eventsHasDataElements(serviceLinkagesDuringQuarter, [
                "sJY2dId52Pv",
            ]);
            const vmmc =
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "LGGiilKOYvf",
                ]) ||
                eventsHasDataElements(homeVisitsDuringQuarter, ["R5ekqgunDq1"]);

            const micro = eventsHasDataElements(serviceLinkagesDuringQuarter, [
                "QzzTM8u8USa",
                "hzuzLSgcOsL",
            ]);

            const operatingAnIGA = anyEventWithDataElement(
                incomeGeneratingActivitiesDuringQuarter,
                "DX0IhFQgsnS",
                "true",
            );

            const igaBooster =
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "Dk5MrVc8DCO",
                ]) ||
                eventsHasDataElements(directBeneficiariesDuringQuarter, [
                    "xTpiUD6hzdU",
                    "w9xIEG3raSL",
                ]);
            const vocationalApprenticeship = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["E7WqYprrglQ"],
            );
            const governmentSocialProtection = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["fyZzO01L4oO", "aAzK9GU72D4"],
            );
            const outputMarkets = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["pJTLmJDJDyA"],
            );
            const supportedToEnroll = eventsHasDataElements(
                serviceLinkagesDuringQuarter,
                ["zugisWwepeI"],
            );

            const igaRegisteringSuccess = anyEventWithAnyOfTheValue(
                incomeGeneratingActivitiesDuringQuarter,
                "TecfD9jSvKy",
                ["50,001-300,000", "300,001-500,000", "Over 500,000"],
            );

            // TODO add Monthly vsla tool
            const tempConsumption =
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "HBOascaLodU",
                    "icbRVzuYeIy",
                    "eoQxge4LVrx",
                ]) ||
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Temporary Food Support"],
                ) ||
                eventsHasDataElements(directBeneficiariesDuringQuarter, [
                    "tuG9ip7SNTD",
                    "LuOuu8760CW",
                    "kxW0SiGb5cO",
                    "eZ1Y3gRXt4k",
                ]);

            const vlsaOvcFund = eventsHasDataElements(
                ["TGgP0ndV5Yj"],
                serviceLinkagesDuringQuarter,
            );
            const educationFund = eventsHasDataElements(
                directBeneficiariesDuringQuarter,
                ["HmpNjCf65od"],
            );
            const scholasticMaterials =
                eventsHasDataElements(directBeneficiariesDuringQuarter, [
                    "pmTpaik3AgA",
                    "eOb7mLpEVsc",
                    "sViPcBarrle",
                    "NUdSDlTPEqt",
                ]) ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "SC6LwbFKBXN",
                ]);
            const schoolFees =
                eventsHasDataElements(directBeneficiariesDuringQuarter, [
                    "HmpNjCf65od",
                    "pmTpaik3AgA",
                    "eOb7mLpEVsc",
                ]) ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "mFv83EyB7Mu",
                ]);
            const educationSubsidy =
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "mFv83EyB7Mu",
                    "SC6LwbFKBXN",
                ]) ||
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Educational support"],
                ) ||
                eventsHasDataElements(directBeneficiariesDuringQuarter, [
                    "Nzm4skzhTGh",
                    "qpMKn9JOiLw",
                    "DmbKoa8UR10",
                    "q7qAlWTOKXz",
                    "SuOs4WcHF13",
                ]);
            // TODO add 2a, 2b, 2c and 2d
            const healthFund = eventsHasDataElements(
                directBeneficiariesDuringQuarter,
                ["umGULhvsBeR", "W0ygjeJ62mX", "NzLj7pJypTz"],
            );

            const educationInformation =
                eventsHasDataElements(
                    homeVisitsDuringQuarter,
                    getSectionDataElements("d6m2LRGqJnn"),
                ) === 1 && age >= 6
                    ? 1
                    : 0;

            const HTSReferral =
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["HCT/ Tested for HIV"],
                ) ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "LwcqnNevUcG",
                ]);

            const nonDisclosureSupport = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["KQIXKWRaWAn", "Ozhhx5kqvFo", "lcLrfWup4jJ", "A4sfodNawYV"],
            );
            const artInitiation = anyEventWithAnyOfTheValue(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                ["Started HIV treatment"],
            );

            const attachedToCorps = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["mCxdn8HSVbn", "bVJ6kjus9QI", "Nm6ccOrQlXW"],
            );

            const homeDrugDelivery = deHasAnyValue(serviceProvided, [
                "Home drug delivery",
            ]);

            const artAdherenceEducation = eventsHasDataElements(
                homeVisitsDuringQuarter,
                [
                    "shpDhNPS54W",
                    "F6dQ1A72dZD",
                    "d36ew0WewuL",
                    "w0sD5QMKbMe",
                    "rdTvCyDsP7v",
                ],
            );
            const hivCareAndLiteracy = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["F6dQ1A72dZD", "w0sD5QMKbMe", "qISFIP4Df6R", "shpDhNPS54W"],
            );

            const iac = getAttribute("iHdNYfm1qlz", currentViralLoad);
            const eMTCT =
                eventsHasDataElements(homeVisitsDuringQuarter, [
                    "AhUJLs4CGMI",
                    "GYxWuJCvCtc",
                    "HF7l5x4f3az",
                ]) ||
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["EMTCT"],
                );

            const hivPrevention = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["WheJwufMW87", "KBFmrSAROjO", "HF7l5x4f3az", "GYxWuJCvCtc"],
            );

            const TFHealth =
                eventsHasDataElements(directBeneficiariesDuringQuarter, [
                    "XT0FoT41zcA",
                    "LQAN64C03mG",
                    "Ltw2hqQFgwu",
                    "mQmBmmGrwnV",
                ]) ||
                eventsHasDataElements(homeVisitsDuringQuarter, [
                    "EG851ch1rWZ",
                    "gLj768y0v9Y",
                    "xdxz9xJ7USt",
                ]);

            const PEP = anyEventWithAnyOfTheValue(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                ["PEP"],
            );

            const covid19Education = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["s4nVMznUGUX", "P1uoG1r4NJc"],
            );

            const immunization = anyEventWithAnyOfTheValue(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                ["Immunisation"],
            );

            const immunisationStatus = getAttribute(
                "RMl0p06BQ9j",
                currentHomeVisit,
            );

            const wash = eventsHasDataElements(homeVisitsDuringQuarter, [
                "t5ruFth9fR8",
                "YylRfk3iKiJ",
                "amaCMIvRteg",
                "e1uS7v9nbKW",
            ]);

            const treatedNets =
                eventsHasDataElements(homeVisitsDuringQuarter, [
                    "qNp1oRUKxB1",
                ]) ||
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Insecticide Treated Nets"],
                ) ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "pAhoqjQAlfE",
                ]);

            const familyPlanning =
                eventsHasDataElements(homeVisitsDuringQuarter, [
                    "oJfyYYOgbqs",
                    "ucTls5HXyN3",
                ]) ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "fSWUj483KL4",
                    "OKRiFPnHs0q",
                    "ndYV4U4popf",
                ]);
            const initiatedOnTB = anyEventWithAnyOfTheValue(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                ["Initiated on TB Treatment"],
            );
            const tested4TB = anyEventWithAnyOfTheValue(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                ["Tested for TB"],
            );

            const supported2CompleteTBDose = anyEventWithAnyOfTheValue(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                ["Supported to Complete TB Dose"],
            );

            const viralLoadBleeding =
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Viral Load Testing"],
                ) === 1 ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "jS1k55KsANg",
                ]);

            const returnedToCare =
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["PLHIV Returned to care", "Provided with ARVs"],
                ) ||
                anyEventWithAnyOfTheValue(
                    missedAppointmentsDuringQuarter,
                    "vnxQFpwvu67",
                    ["Returned to Care"],
                );

            const otherHealthServices = eventsHasDataElements(
                homeVisitsDuringQuarter,
                [
                    "jo02VO05Hh1",
                    "pLoagwClpZC",
                    "shpDhNPS54W",
                    "bDP84oWqmAd",
                    "qISFIP4Df6R",
                    "A4sfodNawYV",
                    "KBFmrSAROjO",
                    "GYxWuJCvCtc",
                    "q2Pop0z4hrt",
                    "kVisFm4hqa1",
                    "sC6Em7tHcLB",
                    "YylRfk3iKiJ",
                    "e1uS7v9nbKW",
                    "TMfxOkHvgJt",
                    "s4nVMznUGUX",
                ],
            );

            const tbScreening =
                (tbScreeningChild >= 4 && age < 16) ||
                (tbScreeningAdult >= 4 && age > 17) ||
                (tbScreeningChild17 >= 4 && age >= 16)
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
                onArt,
                serviceProvided,
            });

            const viralLoadIs12Months = monthsSinceViralTest(
                quarterEnd,
                12,
                lastViralLoadDate,
            );
            const viralLoadIs6Months = monthsSinceViralTest(
                quarterEnd,
                6,
                lastViralLoadDate,
            );

            const {
                missedAppointmentDate,
                missedAnAppointment,
                missedAnAppointmentReason,
                missedAnAppointmentFollowupOutcome,
                hasEverMissedAnAppointment,
                missedAnAppointmentAction,
            } = missedAppointmentInfo(missedAppointments, quarterEnd);

            const VSLA = eventsHasDataElements(
                directBeneficiariesDuringQuarter,
                ["x8BMFjQ1DRf", "H5vsW6LYFhy", "s4w6hTytt5h", "oB05MH3DImM"],
            );

            const ovcProtectionFunds = anyEventWithDE(
                directBeneficiariesDuringQuarter,
                "NyAyKHrhalx",
            )
                ? 1
                : 0;

            const directBeneficiariesOperatingIGA =
                incomeGeneratingActivitiesDuringQuarter.length > 0 ? 1 : 0;

            const coreES = anyService([
                VSLA,
                bankLinkages,
                agricLinkages,
                tempConsumption,
                igaBooster,
                micro,
                vlsaOvcFund,
                VSLABorrowing !== "" && VSLABorrowing !== undefined ? 1 : 0,
                VSLASavings !== "" && VSLASavings !== undefined ? 1 : 0,
                vocationalApprenticeship,
                governmentSocialProtection,
                directBeneficiariesOperatingIGA,
                igaRegisteringSuccess,
                outputMarkets,
            ]);
            const enrolledAtSchool = getAttribute(
                "sMW7nyVNwge",
                currentSchoolMapping,
            );
            const currentSchool = getAttribute(
                "EYTmVQPfoh4",
                currentSchoolMapping,
            );
            const currentClass = getAttribute(
                "pimaAP2qYYE",
                currentSchoolMapping,
            );

            const monitoringAtSchool = eventsHasDataElements(
                ["tKAa4KmRM7R"],
                schoolMonitoringDuringYear,
            );
            const regularlyAttendingSchool = findAnyEventValue(
                schoolMonitoringDuringYear,
                "tKAa4KmRM7R",
            );

            const coreEducation = anyService([
                educationFund,
                educationSubsidy,
                supportedToEnroll,
            ]);

            const communityViralLoadBleeding =
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Viral Load Testing"],
                ) ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "jS1k55KsANg",
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
                GBVScreeningDuringYear,
                "IHcLv90cUNq",
                "true",
            );
            const violence = anyEventWithDataElement(
                GBVScreeningDuringYear,
                "FbF9YMRDarB",
                "true",
            );
            const emotional2 = anyEventWithDataElement(
                GBVScreeningDuringYear,
                "diWuTE7rxUk",
                "true",
            );
            const physicalAbuse = eventsHasDataElements(
                homeVisitsDuringQuarter,
                [
                    "c8SFOQxWyOm",
                    "zOkZh3kDLTZ",
                    "SlVjFqZKz3U",
                    "RSYM3WHKVqT",
                    "wpkk9H2ZiDi",
                    "WP3Jdhog1tQ",
                    "C74BUBGLwNP",
                    "Fwu5MCBQkrQ",
                ],
            );

            // anyEventWithDataElement(
            //     GBVScreeningDuringYear,
            //     "chX1ZE4MQuB",
            //     "true",
            // );

            const sexual1 = anyEventWithDataElement(
                GBVScreeningDuringYear,
                "HZd8eEGyZc4",
                "true",
            );
            const sexual2 = anyEventWithDataElement(
                GBVScreeningDuringYear,
                "UMHo3JZKT5Y",
                "true",
            );
            // const sexualAbuse = sexual1 !== undefined || sexual2 !== undefined;

            const sexualAbuse = eventsHasDataElements(homeVisitsDuringQuarter, [
                "zDvcGqjmSQr",
                "cONYrx9udsm",
                "ruSwtO1J9CV",
                "dMvIT2yhzIR",
                "jrH62qjPmVm",
                "Az2vyIEXkmE",
                "una6BJZTmSq",
                "Byqy2x4IC8J",
            ]);
            const emotionalAbuse = eventsHasDataElements(
                homeVisitsDuringQuarter,
                [
                    "SiwjAG5Z7n1",
                    "mq0VUxyrvQ8",
                    "UXoUP6m5x3n",
                    "Pv5x5K8nysq",
                    "NKeCrNSwWI8",
                    "MpmKR1jTGPa",
                    "GLHrhAoGIjA",
                    "DaIJDBnFIGQ",
                ],
            );

            const reportedGBV =
                (emotional1 !== undefined && emotional1 !== 0) ||
                (emotional2 !== undefined && emotional2 !== 0) ||
                (physicalAbuse !== undefined && physicalAbuse !== 0) ||
                (sexual1 !== undefined && sexual1 !== 0) ||
                (sexual2 !== undefined && sexual2 !== 0) ||
                (violence !== undefined && violence !== 0);

            const hasGbvScreening = getAttributes(
                [
                    "IHcLv90cUNq",
                    "diWuTE7rxUk",
                    "chX1ZE4MQuB",
                    "HZd8eEGyZc4",
                    "UMHo3JZKT5Y",
                ],
                currentGBVScreening,
            ).filter((a) => a !== null && a !== undefined && a !== "");

            const GBVCounseling = eventsHasDataElements(
                GBVScreeningDuringYear,
                ["MPQPmunSbKm"],
            );
            const GBVReferral = eventsHasDataElements(GBVScreeningDuringYear, [
                "CVHBWfo9zcw",
            ]);

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
                    "tfuyHZc81z8",
                ],
            );

            const TFGBV = anyEventWithDataElement(
                referralsDuringQuarter,
                "XWudTD2LTUQ",
                "Transport GBV",
            );

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
                ],
            );
            const GBVLegalSupport =
                eventsHasDataElements(homeVisitsDuringQuarter, [
                    "srAJAZLBf9h",
                    "dvQHNVbEGPb",
                    "KmxUqMzSSWT",
                    "d8d52oyjouy",
                    "c8SFOQxWyOm",
                    "SiwjAG5Z7n1",
                    "zDvcGqjmSQr",
                    "leOn4N7Irta",
                    "vRxHFjnbnwf",
                ]) ||
                anyEventWithDataElement(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    "Legal support",
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

            const basicNeedsDirect = eventsHasDataElements(
                directBeneficiariesDuringQuarter,
                [
                    "Nzm4skzhTGh",
                    "qpMKn9JOiLw",
                    "DmbKoa8UR10",
                    "q7qAlWTOKXz",
                    "JCBi2nMe63P",
                    "umGULhvsBeR",
                    "W0ygjeJ62mX",
                    "NzLj7pJypTz",
                    "Ltw2hqQFgwu",
                    "JWqDIODvcO4",
                ],
            );
            const legalSupport =
                eventsHasDataElements(homeVisitsDuringQuarter, [
                    "zlDhryIk7OU",
                    "y8Pq26t7CyW",
                ]) ||
                anyEventWithDataElement(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    "Legal support",
                ) ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "LD2LNyKiSRh",
                ]);
            const reIntegration = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["Ef9jKdJk9No"],
            );
            const withdrawFromLabour = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["siC6lGt6qOs"],
            );

            const handleChildAbuse = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["AfkLGLnEft4", "y8Pq26t7CyW"],
            );
            const birthRegistration = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["sdoMZC7cD9S"],
            );

            const childProtectionEducation = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["ZekYkAu0olk", "HleyWLT30Rr", "dT7weXodePx", "FTvLP1jSnqT"],
            );
            const GBVEduction = eventsHasDataElements(homeVisitsDuringQuarter, [
                "Fwu5MCBQkrQ",
                "DaIJDBnFIGQ",
                "Byqy2x4IC8J",
                "d6S3dar5jWZ",
                "HNx72j76pnx",
                "mv5c9U6H4d9",
                "qWlv5jmj5xu",
                "YwISL5jAN6K",
                "tfuyHZc81z8",
            ]);
            const coreChildProtection = anyService([
                basicNeed,
                legalSupport,
                reIntegration,
                withdrawFromLabour,
                birthRegistration,
                childProtectionEducation,
                handleChildAbuse,
            ]);
            const nutritionEducation = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["mSruDFpElU1", "uCcUxJtJIcl"],
            );

            const nutritionalStatus = findAnyEventValue(
                homeVisitsDuringQuarter,
                "a3x9L2p3N6u",
            );
            const mentalHealth = findAnyEventValue(
                homeVisitsDuringQuarter,
                "sFgpQOWdusS",
            );
            const nutritionalAssessment = nutritionalStatus ? 1 : 0;
            const voucher4Crops =
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "vFxB1KqM5lp",
                ]) ||
                eventsHasDataElements(homeVisitsDuringQuarter, ["sPzsTkUzj73"]);
            const kitchenGarden = anyEventHasDataElementValue(
                serviceLinkagesDuringQuarter,
                "JJ42NgK9N45",
                "true",
            );
            const nutritionalFoodSupplement =
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "HBOascaLodU",
                ]) ||
                anyEventWithAnyOfTheValue(
                    referralsDuringQuarter,
                    "XWudTD2LTUQ",
                    ["Nutritional support", "Temporary Food Support"],
                );

            const farmingInputs =
                eventsHasDataElements(homeVisitsDuringQuarter, [
                    "W1BGlC1OtTu",
                ]) ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "BjjU0DuSJRJ",
                ]);

            const agricAdvisoryService =
                eventsHasDataElements(homeVisitsDuringQuarter, [
                    "uSFFWSRwLfz",
                    "R9sLOPh4rvo",
                    "Apq9oZy3tN2",
                    "PWObO6CYTOA",
                    "UeqnDPFlDgu",
                    "HyAoFOhEueY",
                    "L7oe5D7vlDj",
                    "uoLomXW1nTu",
                ]) ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "IovXP3fomDO",
                ]);

            const coreNutrition = anyService([
                nutritionEducation,
                kitchenGarden,
                nutritionalAssessment,
                nutritionalFoodSupplement,
                agricAdvisoryService,
                voucher4Crops,
                farmingInputs,
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
                ],
            );

            const linkedToDSDM = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["UlGVB1Za2e6", "AKd0FB51xCK"],
            );
            const appointmentReminding = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["M3csIquN0lC", "pLoagwClpZC"],
            );
            const willWriting =
                eventsHasDataElements(homeVisitsDuringQuarter, [
                    "as9t4IWFo18",
                ]) ||
                eventsHasDataElements(serviceLinkagesDuringQuarter, [
                    "uxwj0u33Wbk",
                ]);
            const assistiveDevices = eventsHasDataElements(
                homeVisitsDuringQuarter,
                ["ctxofPwv89O"],
            );
            const { graduated, preGraduated } = getGraduationAssessment(
                currentGraduationAssessment,
            );

            const quarter = anyService([
                coreES,
                coreEducation,
                coreHealth,
                coreChildProtection,
                coreNutrition,
            ]);

            const psychosocialSupport = quarter;

            const corePSS = psychosocialSupport;

            const CPas =
                [
                    coreES,
                    coreEducation,
                    coreHealth,
                    coreChildProtection,
                    coreNutrition,
                    corePSS,
                ].filter((a) => a === 1).length >= 3
                    ? 1
                    : 0;

            allPreviousLayering = {
                ...allPreviousLayering,
                [qtr]: { quarter, fullyGraduated: graduated, preGraduated },
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

            const isAtisk = eventsHasDataElementsWithValue(
                riskAssessmentsDuringQuarter,
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
                    "XvsUJ8mP0WG",
                    "Y8kX45XGXXI",
                    "NN0M618qUFX",
                    "MH5BGP1Ww2Q",
                    "p3FSiLQ1q6T",
                    "x1bL4w5EsPL",
                    "dunvFwnbGQF",
                    "oI9btGSwA7P",
                ],
                "true",
            );
            const assetOwnership = findAssetOwnership(filtered, quarterEnd);
            const exitedWithGraduation = getGraduationStatus({
                memberStatus,
                quarter,
                OVC_SERV,
                servedInPreviousQuarter,
                newlyEnrolled,
            });

            if (currentHomeVisit && currentHomeVisit.eventDate) {
                currentLayer = {
                    ...currentLayer,
                    currentHomeVisitDate: currentHomeVisit.eventDate,
                };
            }
            const cleanLayer = removeEmptyKeys({
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
                viralLoadStatus: convertViralStatus(viralLoadStatus),
                sampleType,
                onMultiMonthDispensing: convertBoolToYesNo(
                    onMultiMonthDispensing,
                ),
                clientDSDModel,
                currentTBStatus,
                onTBTreatment,
                viralLoadCopies,
                hasThePersonDisclosed: convertBoolToYesNo(
                    hasThePersonDisclosed,
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
                hasGbvScreening: hasGbvScreening.length > 0 ? 1 : 0,
                reportedGBV: convertBoolToNum(reportedGBV),
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
                viralLoadResultsReceived: convertBoolToNum(
                    viralLoadResultsReceived,
                ),
                hivTestDueDate,
                dateHivTestDone,
                hivTestResults,
                finalOutcome,
                pcr,
                newlyReportedPositive,
                newlyEnrolledText,
                riskFactor,
                householdStatus,
                memberStatus:
                    memberStatus === "No Home Visit" &&
                    servedInPreviousQuarter === 1
                        ? "Present But NotServed"
                        : memberStatus,
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
                tbScreeningStatus,
                VLSuppressed:
                    viralLoadStatus === "1" ? 1 : !!viralLoadStatus ? 0 : "",
                immunisationStatus,
                VSLA,
                directBeneficiariesOperatingIGA,
                igaRegisteringSuccess,
                VSLASavings,
                VSLABorrowing,
                outputMarkets,
                fLiteracy: "",
                agriBusiness: agricLinkages,
                spmTraining: "",
                micro,
                igaBooster,
                tempConsumption,
                vlsaOvcFund,
                coreES,
                educationSubsidy,
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
                iac: convertBoolToNum(iac),
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
                TFGBV: convertSearchTo10(TFGBV),
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
                fullyGraduated: graduated,
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
                linkedToDSDM,
                appointmentReminding,
                orgUnit,
                regimen,
                agricAdvisoryService,
                emotionalAbuse,
                sexualAbuse,
                physicalAbuse,
                IPVHIVDisclosure,
                withdrawnFromGVBHousehold,
                GBVLegalSupport: convertSearchTo10(GBVLegalSupport),
                basicNeed,
                legalSupport: convertSearchTo10(legalSupport),
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
                reasonForVisit,
                currentTBPreventionStatus,
                GBVEduction,
                enrolledAtSchool: convertBoolToYesNo(enrolledAtSchool),
                screened4TB: convertBoolToYesNo(screened4TB),
                generated: new Date().toISOString(),
                ovcProtectionFunds,
                scholasticMaterials,
                schoolFees,
                motherBabyPair,
                heiCode,
                microFranchise,
                apprenticeship,
                operatingAnIGA: convertSearchTo10(operatingAnIGA),
                regularlyAttendingSchool,
                districtOfRelocation,
                subCountyOfRelocation,
                CPas,
                isAtisk,
                basicNeedsDirect,
            });
            layering.push(
                fromPairs(
                    Object.entries(cleanLayer).map(([key, value]) => [
                        key,
                        String(value),
                    ]),
                ),
            );
        }
    }
    return layering;
};

const worker = new Worker<QueryDslQueryContainer>(
    "query",
    async (job) => {
        console.log("=============Starting Layering Job ==============");
        try {   
            await scroll3("RDEklSXCD4C", job.data, async (documents) => {
                const allData = await fetchData(documents);
                const layering = generateLayering({
                    ...allData,
                    periods: [
                        dayjs().subtract(12, "quarters"),
                        dayjs().subtract(11, "quarters"),
                        dayjs().subtract(10, "quarters"),
                        dayjs().subtract(9, "quarters"),
                        dayjs().subtract(8, "quarters"),
                        dayjs().subtract(7, "quarters"),
                        dayjs().subtract(6, "quarters"),
                        dayjs().subtract(5, "quarters"),
                        dayjs().subtract(4, "quarters"),
                        dayjs().subtract(3, "quarters"),
                        dayjs().subtract(2, "quarters"),
                        dayjs().subtract(1, "quarters"),
                        dayjs(),
                    ],
                    trackedEntityInstances: documents,
                });
                logger.info("=============Generating Index for Layering==============");
                await indexBulk("layering", layering);
            });
        } 
        catch (err) {
            console.log("Layering job error:",err);
        }
    },
    { connection },
);

worker.on("completed", (job) => {
    console.log(`Layering job ${job.id} has completed!`);
});

worker.on("failed", (job, err) => {
    console.log(`Layering Job ${job?.id} has failed with ${err.message}`);
});
