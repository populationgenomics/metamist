import * as React from "react";

import DocumentationArticle from "./docs/Documentation";
import SwaggerUI from "swagger-ui-react";
import { SampleView } from "./infoViews/SampleView";
import { FamilyView } from "./infoViews/FamilyView";
import { Routes as Switch, Route } from "react-router-dom";
import { ProjectSummary } from "./project/ProjectSummary";
import ProjectsAdmin from "./admin/ProjectsAdmin";

export const Routes = () => {
    return (
        <>
            <Switch>
                <Route path="/documentation">
                    <Route path="" element={<DocumentationArticle />} />
                    <Route path=":id" element={<DocumentationArticle />} />
                </Route>

                <Route
                    path="/swagger"
                    element={<SwaggerUI url="/openapi.json" />}
                />

                <Route path="/project/">
                    <Route path="" element={<ProjectSummary />} />
                    <Route path=":projectName" element={<ProjectSummary />} />
                    <Route
                        path=":projectName/:page"
                        element={<ProjectSummary />}
                    />
                </Route>

                <Route path="admin" element={<ProjectsAdmin />} />

                <Route
                    path="/"
                    element={<DocumentationArticle articleId="index" />}
                />

                <Route
                    path="project/:projectName/sample/:sampleName"
                    element={<SampleView />}
                />
                <Route
                    path="project/:projectName/family/:familyID"
                    element={<FamilyView />}
                />
                <Route
                    path="project/:projectName/participant/:participantName"
                    element={<SampleView />}
                />
            </Switch>
        </>
    );
};
