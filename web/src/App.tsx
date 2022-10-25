import React from "react";

import { BrowserRouter as Router, Link } from "react-router-dom";

import "swagger-ui-react/swagger-ui.css";
import "semantic-ui-css/semantic.min.css";
// this wasn't working, so added import to HTML
// import 'bootstrap/dist/css/bootstrap.min.css'
import { Routes } from "./urls";
import { Searchbar } from "./Search";
import MuckTheDuck from "./MuckTheDuck";

function App() {
    return (
        <Router>
            <div className="App">
                <header className="App-header">
                    <div className="header">
                        <Link className="metamist-img" to="/">
                            <MuckTheDuck height={28} style={{ marginRight: "5px" }} />
                        </Link>
                        <Link className="metamist" to="/">
                            METAMIST
                        </Link>
                        <Link className="navbarLink" to="/project">
                            Explore
                        </Link>
                        <Link className="navbarLink" to="/swagger">
                            Swagger
                        </Link>
                        <Link className="navbarLink" to="/documentation">
                            Docs
                        </Link>
                        <Searchbar />
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
