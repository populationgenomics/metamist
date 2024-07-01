import { Dictionary } from 'lodash'
import { Location } from 'react-router-dom'

const generateUrl = (location: Location, params: Dictionary<string | undefined>): string => {
    let paramsArray: string[] = []
    paramsArray = Object.entries(params)
        .filter(([_, value]) => value !== null && value !== undefined)
        .map(([key, value]) => `${key}=${value}`)

    if (paramsArray.length === 0) return `${location.pathname}`

    return `${location.pathname}?${paramsArray.join('&')}`
}

export default generateUrl
