import React from 'react';


import {
  BrowserRouter as Router,
  Link
} from "react-router-dom";

import "swagger-ui-react/swagger-ui.css"
// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'
import { Routes } from './urls'


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
