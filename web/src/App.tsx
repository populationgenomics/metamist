import React from 'react';
import DocumentationArticle from './Documentation'
import SwaggerUI from 'swagger-ui-react'
import "swagger-ui-react/swagger-ui.css"


import {
  BrowserRouter as Router,
  Routes as Switch,
  Route,
  Link
} from "react-router-dom";

const Index = () => {

  return (<div>
    <h2>Sample metadata server</h2>
    <ul>
      <li><Link to="documentation">Python API documentation</Link></li>
      <li><Link to="swagger">Swagger page</Link></li>
    </ul>

  </div>)
}

const Routes = () => {

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
        element={<SwaggerUI url="http://localhost:8000/openapi.json" />}
      />

      <Route
        path="/"
        element={<Index />}
      />
    </Switch>
  </>
}

function App() {
  return (
    <Router>
      <div className="App">
        <header className="App-header">
          <div className="header">
            <Link to="/">CPG Sample Metadata</Link>
            <Link to="/documentation" >Docs</Link>
          </div>
        </header>
        <div className="body">
          <Routes />
        </div>
      </div>
    </Router>
  );
}

export default App;
