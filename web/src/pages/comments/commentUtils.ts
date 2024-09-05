import { Location } from 'react-router-dom'

const toTitleCase = (str: string) => {
    return str
        .split(' ')
        .map((ss) => `${ss.charAt(0).toLocaleUpperCase()}${ss.substring(1).toLocaleLowerCase()}`)
        .join(' ')
}

const serviceAccountStr = '.iam.gserviceaccount.com'

// Convert the author email to something that is a bit more friendly
// Makes an attempt at converting service accounts to something readable
export const parseAuthor = (author: string) => {
    const authorParts = author.split('@')
    const username = authorParts[0]
    const domain: string | undefined = authorParts[1]
    const isMachineUser = domain ? domain.endsWith(serviceAccountStr) : false

    let name = ''

    if (isMachineUser) {
        name = toTitleCase(
            author.replace(serviceAccountStr, '').replace('@', ' @ ').replace(/-/g, ' ')
        )
    } else {
        name = toTitleCase(username.replace(/[.-]/g, ' '))
    }

    const initials = name
        .split(' ')
        .map((nn) => nn[0])
        .join('')
        .slice(0, 2)

    return {
        username: author,
        isMachineUser,
        name,
        initials,
    }
}

export const getCommentLink = (id: number, location: Location) => {
    const params = new URLSearchParams(location.search)
    params.set('show_comments', 'true')
    params.set('comment_id', id.toString())
    return `${window.location.protocol}//${window.location.host}${location.pathname}?${params.toString()}`
}
