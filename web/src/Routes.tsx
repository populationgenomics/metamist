import * as React from 'react'

import SwaggerUI from 'swagger-ui-react'
import { Routes as Switch, Route } from 'react-router-dom'
import DocumentationArticle from './pages/docs/Documentation'
import SampleView from './pages/sample/SampleView'
import FamilyView from './pages/family/FamilyView'
import ProjectSummary from './pages/project/ProjectSummary'
import ProjectsAdmin from './pages/admin/ProjectsAdmin'
import ErrorBoundary from './shared/utilities/errorBoundary'

const Routes: React.FunctionComponent = () => (
    <Switch>
        <Route path="/documentation">
            <Route path="" element={<DocumentationArticle />} />
            <Route path=":id" element={<DocumentationArticle />} />
        </Route>

        <Route path="/swagger" element={<SwaggerUI url="/openapi.json" />} />

        <Route path="/project/">
            <Route
                path=""
                element={
                    <ErrorBoundary>
                        <ProjectSummary />
                    </ErrorBoundary>
                }
            />
            <Route
                path=":projectName"
                element={
                    <ErrorBoundary>
                        <ProjectSummary />
                    </ErrorBoundary>
                }
            />
            <Route
                path=":projectName/:page"
                element={
                    <ErrorBoundary>
                        <ProjectSummary />
                    </ErrorBoundary>
                }
            />
        </Route>

        <Route path="admin" element={<ProjectsAdmin />} />

        <Route path="/" element={<DocumentationArticle articleid="index" />} />

        <Route
            path="sample/:sampleName"
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

        <Route
            path="project/:projectName/participant/:participantName"
            element={
                <ErrorBoundary>
                    <SampleView />
                </ErrorBoundary>
            }
        />
    </Switch>
)

export default Routes
