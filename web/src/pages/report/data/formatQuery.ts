import { stripIndent } from 'common-tags'

export type NamedQueryPart = {
    name: string
    query: string
}

export type UnformattedQuery = string | NamedQueryPart[]

// Take a list of queries and combine them into a single query using CTEs
// This allows viewing of steps that are used to arrive at a final aggregated query
export function formatQuery(query: UnformattedQuery) {
    if (typeof query === 'string') return stripIndent(query)
    const cleanQueries = query.map((qq) => {
        return {
            ...qq,
            // remove any trailing semicolons from the query, in case they were added accidentally
            query: stripIndent(qq.query.replace(/;[\w\s]*$/g, '')),
        }
    })

    if (cleanQueries.length === 1) return cleanQueries[0].query

    const ctes = cleanQueries.slice(0, cleanQueries.length - 1).map((qq) => {
        // To format the query nicely, need to indent each line by the same amount, but no way
        // of knowing what that amount is, as it depends on the code formatting :/ so use a sentinel
        // to represent the query, then check how indented it is and replace it with the real query
        // while adding the correct indentation to each line (other than the first).
        const querySentinel = `___QUERY_REPLACEMENT___`

        const queryPlaceholder = stripIndent`
            ${qq.name} AS (
                ${querySentinel}
            )
        `

        const matcher = new RegExp(`^(\\s*)${querySentinel}`, 'm')

        const queryIndent = queryPlaceholder.match(matcher)?.[1] || ''

        const indentedQuery = qq.query
            .split('\n')
            .map((line, index) => (index === 0 ? line : queryIndent + line))
            .join('\n')

        return queryPlaceholder.replace(querySentinel, indentedQuery)
    })

    const finalQuery = cleanQueries[cleanQueries.length - 1]

    return stripIndent`WITH ${ctes.join(',\n')}
${finalQuery.query}
    `
}
