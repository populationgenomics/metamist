const formatMoney = (val: number | undefined | null, dp: number = 2): string => {
    const options = {
        style: 'currency',
        currency: 'AUD',
        minimumFractionDigits: dp,
        maximumFractionDigits: dp,
    } as Intl.NumberFormatOptions
    return val ? val.toLocaleString('en-AU', options) : ''
}

export default formatMoney
