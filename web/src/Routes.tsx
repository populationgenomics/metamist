import * as React from 'react'

import SwaggerUI from 'swagger-ui-react'
import { Routes as Switch, Route } from 'react-router-dom'
import DocumentationArticle from './docs/Documentation'
import SampleView from './infoViews/SampleView'
import { FamilyView } from './infoViews/FamilyView'
import ProjectSummary from './project/ProjectSummary'
import ProjectsAdmin from './admin/ProjectsAdmin'

const Routes: React.FunctionComponent = () => (
    <Switch>
        <Route path="/documentation">
            <Route path="" element={<DocumentationArticle />} />
            <Route path=":id" element={<DocumentationArticle />} />
        </Route>

        <Route path="/swagger" element={<SwaggerUI url="/openapi.json" />} />

        <Route path="/project/">
            <Route path="" element={<ProjectSummary />} />
            <Route path=":projectName" element={<ProjectSummary />} />
            <Route path=":projectName/:page" element={<ProjectSummary />} />
        </Route>

        <Route path="admin" element={<ProjectsAdmin />} />

        <Route path="/" element={<DocumentationArticle articleid="index" />} />

        <Route path="project/:projectName/sample/:sampleName" element={<SampleView />} />
        <Route path="project/:projectName/family/:familyID" element={<FamilyView />} />
        <Route path="project/:projectName/participant/:participantName" element={<SampleView />} />
    </Switch>
)

export default Routes
