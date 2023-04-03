import React from 'react'
import ReactDOM from 'react-dom'
import { ApolloProvider, ApolloClient, createHttpLink, InMemoryCache } from '@apollo/client'
import App from './App'
import reportWebVitals from './reportWebVitals'

// make sure our stylesheet is last
import 'swagger-ui-react/swagger-ui.css'
import 'semantic-ui-css/semantic.min.css'
import './index.css'

const httpLink = createHttpLink({
    uri: '/graphql',
})

const client = new ApolloClient({
    link: httpLink,
    cache: new InMemoryCache(),
})

// 4
ReactDOM.render(
    <React.StrictMode>
        <ApolloProvider client={client}>
            <App />
        </ApolloProvider>
    </React.StrictMode>,
    document.getElementById('root')
)

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals()
