import * as React from 'react'

import SwaggerUI from 'swagger-ui-react'
import { Routes as Switch, Route } from 'react-router-dom'
import DocumentationArticle from './docs/Documentation'
import DetailedInfoPage from './DetailedInfoPage'
import ProjectSummary from './project/ProjectSummary'
import ProjectsAdmin from './admin/ProjectsAdmin'

const Routes: React.FunctionComponent = () => (
    <Switch>
        <Route path="/documentation">
            <Route path="" element={<DocumentationArticle />} />
            <Route path=":id" element={<DocumentationArticle />} />
        </Route>

        <Route
            path="/swagger"
            element={<SwaggerUI url="/openapi.json" />}
        />

        <Route path="project/" element={<ProjectSummary />} />
        <Route path="project/:projectName" element={<ProjectSummary />} />
        <Route
            path="project/:projectName/:page/"
            element={<ProjectSummary />}
        />

        <Route path="admin" element={<ProjectsAdmin />} />

        <Route
            path="/"
            element={<DocumentationArticle articleid="index" />}
        />

        <Route
            path="project/:projectName/sample/:sampleName"
            element={<DetailedInfoPage />}
        />
        <Route
            path="project/:projectName/family/:familyName"
            element={<DetailedInfoPage />}
        />
        <Route
            path="project/:projectName/participant/:participantName"
            element={<DetailedInfoPage />}
        />
    </Switch>
)

export default Routes
