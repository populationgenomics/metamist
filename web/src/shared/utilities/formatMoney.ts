const formatMoney = (val: number | undefined, dp: number = 2): string => val ? `$${val.toFixed(dp).replace(/\d(?=(\d{3})+\.)/g, '$&,')}` : ''

export default formatMoney
