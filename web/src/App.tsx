import React, { useEffect } from 'react';
import logo from './logo.svg';
import DocumentationArticle from './Documentation'
import SwaggerUI from 'swagger-ui-react'
import "swagger-ui-react/swagger-ui.css"


import {
  BrowserRouter as Router,
  Routes as Switch,
  Route,
  Link
} from "react-router-dom";
import ReactMarkdown from 'react-markdown';
import { setUncaughtExceptionCaptureCallback } from 'process';

const Index = () => {

  const [readmeText, setREADME] = React.useState('')

  // useEffect(() => {
  //   // async function loadText()
  //   function loadData() {
  //     return import('../public/sm-readme.md')
  //       .then(file => fetch(file.default))
  //       .then(response => response.text())
  //       .then(text => setREADME(text))
  //   }
  //   loadData()
  // })


  return (<div>
    <h2>Sample metadata server</h2>
    <ul>
      <li><Link to="documentation">Python API documentation</Link></li>
      <li><Link to="swagger">Swagger page</Link></li>
    </ul>

    <ReactMarkdown children={readmeText} />
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
          <Link to="/">CPG Sample Metadata</Link>
          <Link to="/documentation" >Docs</Link>
        </header>
        <div className="body">
          <Routes />
        </div>
      </div>
    </Router>
  );
}

export default App;
