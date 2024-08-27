import { ApolloClient, ApolloProvider, createHttpLink, InMemoryCache } from '@apollo/client'
import React from 'react'
import ReactDOM from 'react-dom'
import App from './App'
import reportWebVitals from './reportWebVitals'

// make sure our stylesheet is last
import 'semantic-ui-css/semantic.min.css'
import 'swagger-ui-react/swagger-ui.css'
import './index.css'
import { ThemeProvider } from './shared/components/ThemeProvider'

const httpLink = createHttpLink({
    uri: '/graphql',
})

const client = new ApolloClient({
    link: httpLink,
    cache: new InMemoryCache({
        possibleTypes: {
            // Not sure why apollo can't get this from the schema,
            // but this is required so that fields on the union
            // types are retrievable.
            GraphQLCommentEntity: [
                'GraphQLSample',
                'GraphQLAssay',
                'GraphQLSequencingGroup',
                'GraphQLProject',
                'GraphQLParticipant',
                'GraphQLFamily',
            ],
        },
    }),
})

const storedTheme = localStorage.getItem('theme') || 'light-mode'
if (storedTheme) document.documentElement.setAttribute('data-theme', storedTheme)

ReactDOM.render(
    <React.StrictMode>
        <ApolloProvider client={client}>
            <ThemeProvider>
                <App />
            </ThemeProvider>
        </ApolloProvider>
    </React.StrictMode>,
    document.getElementById('root')
)

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals()
