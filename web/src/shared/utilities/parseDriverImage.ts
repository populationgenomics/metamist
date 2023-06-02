const parseDriverImage = (value: string): string => {
    const splits = value.split('/')
    const driverName = splits[splits.length - 1]
    const splitName = driverName.split(':')
    return `${splitName[0]}:${splitName[1].substring(0, 7)}`
}
export default parseDriverImage
