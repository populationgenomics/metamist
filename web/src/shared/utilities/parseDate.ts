const parseDate = (value: string): string => {
    const date = new Date(value)
    return `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`
}
export default parseDate
