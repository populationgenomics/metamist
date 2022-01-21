import React from 'react';

import DocumentationArticle from './docs/Documentation'
import SwaggerUI from 'swagger-ui-react'
import {
    Routes as Switch,
    Route,
    Link
} from "react-router-dom";
import { ProjectSummary } from './project/ProjectSummary';


const Index = () => {

    return (<div>
        <h2>Sample metadata server</h2>
        <ul>
            <li><Link to="documentation">Python API documentation</Link></li>
            <li><Link to="swagger">Swagger page</Link></li>
            <li><Link to="project">Samples table</Link></li>
        </ul>

        <DocumentationArticle articleId="index" />

    </div>)
}

export const Routes = () => {

    return <>
        <Switch>
            <Route
                path="/documentation"
            >
                <Route path="" element={<DocumentationArticle />} />
                <Route path=":id" element={<DocumentationArticle />} />
            </Route>

            <Route
                path="/swagger"
                element={<SwaggerUI url="/openapi.json" />}
            />

            <Route path="project" element={<ProjectSummary />} />

            <Route
                path="/"
                element={<Index />}
            />
        </Switch>
    </>
}
