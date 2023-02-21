const sanitiseValue = (value: any) => {
    const tvalue = typeof value
    if (tvalue === 'string' || tvalue === 'number') return value
    if (tvalue === 'boolean') return value ? 'true' : 'false'
    if (value === undefined || value === null) return ''
    return JSON.stringify(value)
}

export default sanitiseValue
