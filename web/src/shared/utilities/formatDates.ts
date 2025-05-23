const getAdjustedDay = (value: string, days: number): string => {
    const date = new Date(value)
    date.setDate(date.getDate() + days)
    return [
        date.getFullYear(),
        (date.getMonth() + 1).toString().padStart(2, '0'),
        date.getDate().toString().padStart(2, '0'),
    ].join('-')
}

const getCurrentInvoiceMonth = () => {
    // get current month and year in the format YYYYMM
    const date = new Date()
    return [date.getFullYear(), (date.getMonth() + 1).toString().padStart(2, '0')].join('')
}

const getCurrentInvoiceYearStart = () => {
    const date = new Date()
    return [date.getFullYear(), '01'].join('')
}

const generateInvoiceMonths = (start: string, end: string): string[] => {
    const invoiceMonths = []
    const yearStart = parseInt(start.substring(0, 4))
    const yearEnd = parseInt(end.substring(0, 4))

    const mthStart = start.substring(4, 6).padStart(2, '0')
    const mthEnd = end.substring(4, 6).padStart(2, '0')

    const dateStart = new Date(yearStart + '-' + mthStart + '-01')
    const dateEnd = new Date(yearEnd + '-' + mthEnd + '-01')

    for (let i = yearStart; i <= yearEnd; i++) {
        const startMonth = i === yearStart ? dateStart.getMonth() : 0
        const endMonth = i === yearEnd ? dateEnd.getMonth() : 11
        for (let j = startMonth; j <= endMonth; j++) {
            const month = j + 1
            const monthString = month.toString().padStart(2, '0')
            const yearString = i.toString()
            const dateString = `${yearString}${monthString}`
            invoiceMonths.push(dateString)
        }
    }
    return invoiceMonths
}

export { generateInvoiceMonths, getAdjustedDay, getCurrentInvoiceMonth, getCurrentInvoiceYearStart }
