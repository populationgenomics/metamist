import { Dictionary } from 'lodash'

const generateUrl = (location: Location, params: Dictionary<string>): string => {
    let paramsArray: string[] = []
    paramsArray = Object.entries(params)
        .filter(([_, value]) => value !== null && value !== undefined)
        .map(([key, value]) => `${key}=${value}`)

    if (paramsArray.length === 0)
        return `${location.pathname}`

    return `${location.pathname}?${paramsArray.join('&')}`
}

export default generateUrl
