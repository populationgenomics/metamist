import formatBytes from './formatBytes'

const safeValue = (value: unknown): string => {
    if (!value) return JSON.stringify(value)
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
        if (
            Object.prototype.hasOwnProperty.call(value, 'location') &&
            Object.prototype.hasOwnProperty.call(value, 'size')
        ) {
            const valueAsObject = value as { location: string; size: number }
            return `${valueAsObject.location} (${formatBytes(valueAsObject.size)})`
        }
    }
    return JSON.stringify(value)
}

export default safeValue
