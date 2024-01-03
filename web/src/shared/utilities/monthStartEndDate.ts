const getMonthStartDate = (): string => {
    const now = new Date()
    return `${now.getFullYear()}-${(now.getMonth() + 1).toString().padStart(2, '0')}-01`
}

const getMonthEndDate = (): string => {
    const now = new Date()
    return [
        now.getFullYear(),
        (now.getMonth() + 1).toString().padStart(2, '0'),
        now.getDate().toString().padStart(2, '0')
    ].join('-')
}

export {getMonthStartDate, getMonthEndDate}