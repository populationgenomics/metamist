const sanitiseValue = (value: unknown): string => {
    const tvalue = typeof value
    if (tvalue === 'string') return value as string
    if (tvalue === 'number') return JSON.stringify(value)
    if (tvalue === 'boolean') return value ? 'true' : 'false'
    if (value === undefined || value === null) return ''
    return JSON.stringify(value)
}

export default sanitiseValue
