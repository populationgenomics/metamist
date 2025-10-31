import * as React from 'react'

import { Route, Routes as Switch } from 'react-router-dom'
import SwaggerUI from 'swagger-ui-react'

import ProjectsAdmin from './pages/admin/ProjectsAdmin'
import { AnalysisViewPage } from './pages/analysis/AnalysisView'
import {
    BillingCostByAnalysis,
    BillingCostByCategory,
    BillingCostByMonth,
    BillingCostBySample,
    BillingCostByTime,
    BillingHome,
    BillingInvoiceMonthCost,
    BillingSeqrProp,
    SequencingGroupsByMonth,
} from './pages/billing'
import DocumentationArticle from './pages/docs/Documentation'
import { FamilyPage } from './pages/family/FamilyView'
import Details from './pages/insights/Details'
import Summary from './pages/insights/Summary'
import { ParticipantPage } from './pages/participant/ParticipantViewContainer'
import AnalysisRunnerSummary from './pages/project/AnalysisRunnerView/AnalysisRunnerSummary'
import ProjectOverview from './pages/project/ProjectOverview'
import ProjectReport from './pages/report/ProjectReport'
import SqlQueryUI from './pages/report/SqlQueryUI'
import SampleView from './pages/sample/SampleView'
import ErrorBoundary from './shared/utilities/errorBoundary'

const Routes: React.FunctionComponent = () => (
    <Switch>
        <Route path="/" element={<DocumentationArticle articleid="index" />} />
        {/* <Route path="/tt" element={<TangledTreeExamples />} /> */}

        <Route path="admin" element={<ProjectsAdmin />} />
        <Route
            path="/project/:projectName?/:page?"
            element={
                <ErrorBoundary>
                    <ProjectOverview />
                </ErrorBoundary>
            }
        />

        <Route
            path="/project/:projectName/query/:tableName?"
            element={
                <ErrorBoundary>
                    <SqlQueryUI />
                </ErrorBoundary>
            }
        />
        <Route
            path="/project/:projectName/report/:reportName/:tabName?"
            element={
                <ErrorBoundary>
                    <ProjectReport />
                </ErrorBoundary>
            }
        />
        <Route
            path="/participant/:participantId"
            element={
                <ErrorBoundary>
                    <ParticipantPage />
                </ErrorBoundary>
            }
        />
        <Route
            path="/analysis-runner/:projectName?"
            element={
                <ErrorBoundary>
                    <AnalysisRunnerSummary />
                </ErrorBoundary>
            }
        />

        <Route path="/billing/" element={<BillingHome />} />
        <Route
            path="/billing/costByMonth"
            element={
                <ErrorBoundary>
                    <BillingCostByMonth />
                </ErrorBoundary>
            }
        />
        <Route path="/billing/invoiceMonthCost" element={<BillingInvoiceMonthCost />} />
        <Route
            path="/billing/costByTime"
            element={
                <ErrorBoundary>
                    <BillingCostByTime />
                </ErrorBoundary>
            }
        />
        <Route
            path="/billing/costByAnalysis"
            element={
                <ErrorBoundary>
                    <BillingCostByAnalysis />
                </ErrorBoundary>
            }
        />
        <Route
            path="/billing/costByCategory"
            element={
                <ErrorBoundary>
                    <BillingCostByCategory />
                </ErrorBoundary>
            }
        />
        <Route
            path="/billing/costBySample"
            element={
                <ErrorBoundary>
                    <BillingCostBySample />
                </ErrorBoundary>
            }
        />
        <Route
            path="/billing/seqrPropMap"
            element={
                <ErrorBoundary>
                    <BillingSeqrProp />
                </ErrorBoundary>
            }
        />
        <Route
            path="/billing/sequencingGroupsByMonth/:projectName?"
            element={<SequencingGroupsByMonth />}
        />

        <Route path="/swagger" element={<SwaggerUI url="/openapi.json" tryItOutEnabled={true} />} />

        <Route path="/documentation/:id?" element={<DocumentationArticle />} />

        <Route
            path="/sample/:sampleName/:sequencingGroupName?"
            element={
                <ErrorBoundary>
                    <SampleView />
                </ErrorBoundary>
            }
        />

        <Route
            path="/family/:familyId"
            element={
                <ErrorBoundary>
                    <FamilyPage />
                </ErrorBoundary>
            }
        />

        <Route
            path="/analysis/:analysisId"
            element={
                <ErrorBoundary>
                    <AnalysisViewPage />
                </ErrorBoundary>
            }
        />

        <Route
            path="insights/details"
            element={
                <ErrorBoundary>
                    <Details />
                </ErrorBoundary>
            }
        />

        <Route
            path="insights/summary"
            element={
                <ErrorBoundary>
                    <Summary />
                </ErrorBoundary>
            }
        />
    </Switch>
)

export default Routes
