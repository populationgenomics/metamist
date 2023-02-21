import formatBytes from './formatBytes'

const safeValue = (value: any): string => {
    if (!value) return value
    if (Array.isArray(value)) {
        return value.map(safeValue).join(', ')
    }
    if (typeof value === 'number') {
        return value.toString()
    }
    if (typeof value === 'string') {
        return value
    }
    if (value && typeof value === 'object' && !Array.isArray(value)) {
        if (!!value.location && !!value.size) {
            return `${value.location} (${formatBytes(value.size)})`
        }
    }
    return JSON.stringify(value)
}

export default safeValue
