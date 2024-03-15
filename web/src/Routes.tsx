import * as React from 'react'

import SwaggerUI from 'swagger-ui-react'
import { Routes as Switch, Route } from 'react-router-dom'
import {
    BillingHome,
    BillingSeqrProp,
    BillingCostByTime,
    BillingCostByAnalysis,
    BillingInvoiceMonthCost,
    BillingCostByCategory,
    BillingCostByMonth,
} from './pages/billing'
import DocumentationArticle from './pages/docs/Documentation'
import SampleView from './pages/sample/SampleView'
import FamilyView from './pages/family/FamilyView'
import ProjectSummaryView from './pages/project/ProjectSummary'
import ProjectsAdmin from './pages/admin/ProjectsAdmin'
import ErrorBoundary from './shared/utilities/errorBoundary'
import AnalysisRunnerSummary from './pages/project/AnalysisRunnerView/AnalysisRunnerSummary'

const Routes: React.FunctionComponent = () => (
    <Switch>
        <Route path="/" element={<DocumentationArticle articleid="index" />} />

        <Route path="admin" element={<ProjectsAdmin />} />
        <Route
            path="/project/:projectName?/:page?"
            element={
                <ErrorBoundary>
                    <ProjectSummaryView />
                </ErrorBoundary>
            }
        />
        <Route
            path="project/:projectName/participant/:participantName"
            element={
                <ErrorBoundary>
                    <SampleView />
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
            path="/billing/seqrPropMap"
            element={
                <ErrorBoundary>
                    <BillingSeqrProp />
                </ErrorBoundary>
            }
        />

        <Route path="/swagger" element={<SwaggerUI url="/openapi.json" tryItOutEnabled={true} />} />

        <Route path="/documentation/:id?" element={<DocumentationArticle />} />

        <Route
            path="sample/:sampleName/:sequencingGroupName?"
            element={
                <ErrorBoundary>
                    <SampleView />
                </ErrorBoundary>
            }
        />

        <Route
            path="/family/:familyID"
            element={
                <ErrorBoundary>
                    <FamilyView />
                </ErrorBoundary>
            }
        />
    </Switch>
)

export default Routes
