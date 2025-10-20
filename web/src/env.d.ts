// Group of GCP projects
type BillingProjectGroup = {
    name: string
    groupBy: string
    gcpProjects: string
}

// Group of topics
type BillingTopicGroup = {
    name: string
    groupBy: string
    topics: string
}

type BillingTeamInfo = {
    teamName: string
    billingGroups: (BillingProjectGroup | BillingTopicGroup)[]
}

declare const PROJECT_GROUPS: BillingTeamInfo[]
