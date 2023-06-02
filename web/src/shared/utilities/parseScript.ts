const parseScript = (value: string): string =>
    value.length < 50 ? value : `${value.substring(0, 50)}...`
export default parseScript
